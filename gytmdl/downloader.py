from __future__ import annotations

import datetime
import functools
import io
import re
import os
import shutil
import subprocess
import typing
import http.cookiejar
from pathlib import Path

import requests
from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from mutagen.mp4 import MP4, MP4Cover
from PIL import Image
from yt_dlp import YoutubeDL
from yt_dlp.extractor.youtube import YoutubeTabIE
from ytmusicapi import YTMusic

from .constants import IMAGE_FILE_EXTENSION_MAP, MP4_TAGS_MAP
from .enums import CoverFormat, DownloadMode


class Downloader:
    def __init__(
        self,
        premium: bool,
        all: bool,
        output_path: Path = Path("./YouTube Music"),
        temp_path: Path = Path("./temp"),
        cookies_path: Path = None,
        ffmpeg_path: str = "ffmpeg",
        aria2c_path: str = "aria2c",
        itag: str = "140",
        download_mode: DownloadMode = DownloadMode.YTDLP,
        cover_size: int = "1200",
        cover_format: CoverFormat = CoverFormat.JPG,
        cover_quality: int = 94,
        template_folder: str = "{album_artist}/{album}",
        template_file: str = "{track:02d} {title}",
        template_date: str = "%Y-%m-%dT%H:%M:%SZ",
        exclude_tags: str = None,
        truncate: int = None,
        oauth_path: str = None,
        silent: bool = False,
    ):
        self.m4a128 = 0
        self.m4a64 = 0
        self.m4a = 0
        self.opus128 = 0
        self.opus64 = 0
        self.opus = 0
        self.premium = premium
        self.all = all
        self.output_path = output_path
        self.temp_path = temp_path
        self.cookies_path = cookies_path
        self.ffmpeg_path = ffmpeg_path
        self.aria2c_path = aria2c_path
        self.itag = itag
        self.download_mode = download_mode
        self.cover_size = cover_size
        self.cover_format = cover_format
        self.cover_quality = cover_quality
        self.template_folder = template_folder
        self.template_file = template_file
        self.template_date = template_date
        self.exclude_tags = exclude_tags
        self.truncate = truncate
        self.silent = silent
        self.oauth_path = oauth_path
        self.playlist_id = None
        self.artist = None
        self.selected = []
        self._set_ytmusic_instance()
        self._set_ytdlp_options()
        self._set_exclude_tags()
        self._set_truncate()

    def _set_ytmusic_instance(self):
        self.ytmusic = YTMusic(self.oauth_path)
        # print(self.oauth_path)

    def _set_ytdlp_options(self):
        self.ytdlp_options = {
            "quiet": True,
            "no_warnings": True,
            "noprogress": self.silent,
            "allowed_extractors": ["youtube", "youtube:tab"],
            "extractor_args": {
                "youtube": {
                    "player_client": ["android"],
                },
            },
        }
        if self.cookies_path is not None:
            self.ytdlp_options["cookiefile"] = str(self.cookies_path)

    def _set_exclude_tags(self):
        self.exclude_tags = (
            [i.lower() for i in self.exclude_tags.split(",")]
            if self.exclude_tags is not None
            else []
        )

    def _set_truncate(self):
        if self.truncate is not None:
            self.truncate = None if self.truncate < 4 else self.truncate


    def _get_ytdlp_info(self, url: str) -> dict:
        ytdlp_options = {
            **self.ytdlp_options,
            "extract_flat": True,
            "cookiefile": str(self.cookies_path) if self.cookies_path else None,
        }
        if url == "https://www.youtube.com/playlist?list=None":
            url = self.playlist_id
        if "/browse/" not in url:
            self.playlist_id = url

        print(f"make sure the cookies are for this subdomain => {url}")
        with YoutubeDL(ytdlp_options) as ydl:
            data = ydl.extract_info(url, download=False)
            # print(data)
            return data
    
    def get_download_queue(
        self,
        url: str,
        download_index=-1
    ) -> typing.Generator[dict, None, None]:
        artist_match = re.match(YoutubeTabIE._VALID_URL, url)
        if artist_match and artist_match.group("channel_type") == "channel" and download_index == -2:
            yield self._get_download_queue_artist(artist_match.group("id"), download_index)
        if artist_match and artist_match.group("channel_type") == "channel":
            yield from self._get_download_queue_artist(artist_match.group("id"), download_index)
        else:
            yield from self._get_download_queue_url(url)

    def _get_download_queue_url(
        self,
        url: str,
    ) -> typing.Generator[dict, None, None]:
        ytdlp_info = self._get_ytdlp_info(url.split("&")[0])
        if "MPREb_" in ytdlp_info["webpage_url_basename"]:
            ytdlp_info = self._get_ytdlp_info(ytdlp_info["url"])
        if "playlist" in ytdlp_info["webpage_url_basename"]:
            for entry in ytdlp_info["entries"]:
                yield entry
        if "watch" in ytdlp_info["webpage_url_basename"]:
            yield ytdlp_info

    def get_number_of_albums_and_singles(
        self,
        url: str,
        ):
        artist_match = re.match(YoutubeTabIE._VALID_URL, url)
        artist = self.artist = self.ytmusic.get_artist(artist_match.group("id"))
        # print(artist)
        if self.all:
            # Get albums if available
            if artist.get("albums", {}).get("results"):
                albums = artist["albums"]["results"]
            elif artist.get("albums", {}).get("browseId") and artist.get("albums", {}).get("params"):
                albums = self.ytmusic.get_artist_albums(artist["albums"]["browseId"], artist["albums"]["params"])
            self.selected.extend(albums)

            # Get singles if available
            if artist.get("singles", {}).get("results"):
                singles = artist["singles"]["results"]
            elif artist.get("singles", {}).get("browseId") and artist.get("singles", {}).get("params"):
                singles = self.ytmusic.get_artist_albums(artist["singles"]["browseId"], artist["singles"]["params"])
            self.selected.extend(singles)
        else:
            media_type = inquirer.select(
                message=f'Select which type to download for artist "{artist["name"]}":',
                choices=[
                    Choice(
                        name="Albums",
                        value="albums",
                    ),
                    Choice(
                        name="Singles",
                        value="singles",
                    ),
                ],
                validate=lambda result: artist.get(result, {}).get("results"),
                invalid_message="The artist doesn't have any items of this type",
            ).execute()
            artist_albums = (
                self.ytmusic.get_artist_albums(
                    artist[media_type]["browseId"], artist[media_type]["params"]
                )
                if artist[media_type].get("browseId") and artist[media_type].get("params")
                else artist[media_type]["results"]
            )
            choices = [
                Choice(
                    name=" | ".join(
                        [
                            album.get("year", "Unknown"),
                            album["title"],
                        ]
                    ),
                    value=album,
                )
                for album in artist_albums
            ]
            self.selected = inquirer.select(
                message="Select which items to download: (Year | Title)",
                choices=choices,
                multiselect=True,
            ).execute()
        print(len(self.selected))
        return len(self.selected)

    def _get_download_queue_artist(
        self,
        channel_id: str,
        download_index=-1,
    ) -> typing.Generator[dict, None, None]:
        album = self.selected[download_index]
        yield from self._get_download_queue_url(
                "https://music.youtube.com/browse/" + album["browseId"]
            )

    @staticmethod
    def _get_artist(artist_list: dict) -> str:
        if len(artist_list) == 1:
            return artist_list[0]["name"]
        return (
            ", ".join([i["name"] for i in artist_list][:-1])
            + f' & {artist_list[-1]["name"]}'
        )

    def get_ytmusic_watch_playlist(self, video_id: str) -> dict | None:
        ytmusic_watch_playlist = self.ytmusic.get_watch_playlist(video_id)
        if not ytmusic_watch_playlist["tracks"][0].get("album"):
            return None
        return ytmusic_watch_playlist

    @functools.lru_cache()
    def get_ytmusic_album(self, browse_id: str) -> dict:
        return self.ytmusic.get_album(browse_id)

    @staticmethod
    def _get_datetime_obj(date: str) -> datetime.datetime:
        return datetime.datetime.strptime(date, "%Y")

    def get_tags(self, ytmusic_watch_playlist: dict) -> dict:
        video_id = ytmusic_watch_playlist["tracks"][0]["videoId"]
        ytmusic_album = self.get_ytmusic_album(
            ytmusic_watch_playlist["tracks"][0]["album"]["id"]
        )
        # print(ytmusic_watch_playlist["tracks"][0]["album"]["id"])
        # print(ytmusic_album)
        tags = {
            "album": ytmusic_album["title"],
            "album_artist": self._get_artist(ytmusic_album["artists"]),
            "artist": self._get_artist(ytmusic_watch_playlist["tracks"][0]["artists"]),
            "url": f"https://music.youtube.com/watch?v={video_id}",
            "media_type": 1,
            "title": ytmusic_watch_playlist["tracks"][0]["title"],
            "track_total": ytmusic_album["trackCount"],
        }
        extracted_info = self._get_ytdlp_info(
                f'https://www.youtube.com/playlist?list={ytmusic_album["audioPlaylistId"]}'
            )
        entries = extracted_info.get("entries", [])
        if not entries: # then it's not a playlist!!! it should be a watch link
            entries = [extracted_info]
        for index, entry in enumerate(entries
        ):
            if entry["id"] == video_id:
                if ytmusic_album["tracks"][index]["isExplicit"]:
                    tags["rating"] = 1
                else:
                    tags["rating"] = 0
                tags["track"] = index + 1
                break
            # print(entry["id"], video_id, index)
        if ytmusic_watch_playlist["lyrics"]:
            lyrics = self.ytmusic.get_lyrics(ytmusic_watch_playlist["lyrics"])["lyrics"]
            if lyrics is not None:
                tags["lyrics"] = lyrics
        datetime_obj = (
            self._get_datetime_obj(ytmusic_album["year"])
            if ytmusic_album.get("year")
            else None
        )
        if datetime_obj:
            tags["date"] = datetime_obj.strftime(self.template_date)
        return tags

    def get_sanitized_string(self, dirty_string: str, is_folder: bool) -> str:
        dirty_string = re.sub(r'[\\/:*?"<>|;]', "_", dirty_string)
        if is_folder:
            dirty_string = dirty_string[: self.truncate]
            if dirty_string.endswith("."):
                dirty_string = dirty_string[:-1] + "_"
        else:
            if self.truncate is not None:
                dirty_string = dirty_string[: self.truncate - 4]
        return dirty_string.strip()

    def get_track_temp_path(self, video_id: str) -> Path:
        return self.temp_path / f"{video_id}_temp.m4a"

    def get_remuxed_path(self, video_id: str) -> Path:
        return self.temp_path / f"{video_id}_remuxed.m4a"

    def get_cover_path(self, final_path: Path, file_extension: str) -> Path:
        return final_path.parent / ("Cover" + file_extension)

    def get_final_path(self, tags: dict) -> Path:
        final_path_folder = self.template_folder.split("/")
        final_path_file = self.template_file.split("/")
        final_path_folder = [
            self.get_sanitized_string(i.format(**tags), True) for i in final_path_folder
        ]
        final_path_file = [
            self.get_sanitized_string(i.format(**tags), True)
            for i in final_path_file[:-1]
        ] + [
            self.get_sanitized_string(final_path_file[-1].format(**tags), False)
            + ".m4a"
        ]
        return self.output_path.joinpath(*final_path_folder).joinpath(*final_path_file)

    def download(self, video_id: str, temp_path: Path):
        options = {
                    **self.ytdlp_options,
                    "external_downloader": (
                        {
                            "default": self.aria2c_path,
                        }
                        if self.download_mode == DownloadMode.ARIA2C
                        else None
                    ),
                    "fixup": "never",
                    "format": self.itag,
                    "outtmpl": str(temp_path),
                    "cookiefile": str(self.cookies_path) if self.cookies_path else None,  # Include cookies
                }
        if self.premium:
            options["format"] = "141"
            with YoutubeDL(options) as ydl:
                ydl.download("https://music.youtube.com/watch?v=" + video_id)
            if not os.path.exists(str(temp_path)):
                options["format"] = "774"
                with YoutubeDL(options) as ydl:
                    ydl.download("https://music.youtube.com/watch?v=" + video_id)
                if not os.path.exists(str(temp_path)):
                    options["format"] = "140"
                    with YoutubeDL(options) as ydl:
                        ydl.download("https://music.youtube.com/watch?v=" + video_id)
                    if not os.path.exists(str(temp_path)):
                        options["format"] = "251"
                        with YoutubeDL(options) as ydl:
                            ydl.download("https://music.youtube.com/watch?v=" + video_id)
                        if not os.path.exists(str(temp_path)):
                            options["format"] = "139"
                            with YoutubeDL(options) as ydl:
                                ydl.download("https://music.youtube.com/watch?v=" + video_id)
                            if not os.path.exists(str(temp_path)):
                                options["format"] = "250"
                                with YoutubeDL(options) as ydl:
                                    ydl.download("https://music.youtube.com/watch?v=" + video_id)
                                print("downloading format opus 250 => 64kb/s")
                                self.opus64 += 1
                            else:
                                print("downloading format m4a 139 => 64kb/s")
                                self.m4a64 += 1
                        else:
                            print("downloading format opus 251 => 128kb/s")
                            self.opus128 += 1
                    else:
                        print("downloading format m4a 140 => 128kb/s")
                        self.m4a128 += 1
                else:
                    print("downloading format opus 774 => 256kb/s")
                    self.opus += 1
            else:
                print("downloading format m4a 141 => 256kb/s")
                self.m4a += 1
        else:
            with YoutubeDL(options) as ydl:
                ydl.download("https://music.youtube.com/watch?v=" + video_id)
            if not os.path.exists(str(temp_path)):
                print("specified itag not available")

    def remux(self, temp_path: Path, remuxed_path: Path):
        command = [
            self.ffmpeg_path,
            "-loglevel",
            "error",
            "-i",
            temp_path,
        ]
        if self.itag not in ("141", "140", "139"):
            command.extend(
                [
                    "-f",
                    "mp4",
                ]
            )
        subprocess.run(
            [
                *command,
                "-movflags",
                "+faststart",
                "-c",
                "copy",
                remuxed_path,
            ],
            check=True,
        )

    @staticmethod
    @functools.lru_cache()
    def get_url_response_bytes(url: str) -> bytes:
        return requests.get(url).content

    def get_cover_url(self, ytmusic_watch_playlist: dict) -> str:
        return (
            f'{ytmusic_watch_playlist["tracks"][0]["thumbnail"][0]["url"].split("=")[0]}'
            + (
                "=d"
                if self.cover_format == CoverFormat.RAW
                else f'=w{self.cover_size}-l{self.cover_quality}-{"rj" if self.cover_format == CoverFormat.JPG else "rp"}'
            )
        )

    def get_cover_file_extension(self, cover_url: str) -> str:
        image_obj = Image.open(io.BytesIO(self.get_url_response_bytes(cover_url)))
        image_format = image_obj.format.lower()
        return IMAGE_FILE_EXTENSION_MAP.get(image_format, f".{image_format}")

    def apply_tags(
        self,
        path: Path,
        tags: dict,
        cover_url: str,
    ):
        to_apply_tags = [
            tag_name for tag_name in tags.keys() if tag_name not in self.exclude_tags
        ]
        mp4_tags = {}
        for tag_name in to_apply_tags:
            if tag_name in ("disc", "disc_total"):
                if mp4_tags.get("disk") is None:
                    mp4_tags["disk"] = [[0, 0]]
                if tag_name == "disc":
                    mp4_tags["disk"][0][0] = tags[tag_name]
                elif tag_name == "disc_total":
                    mp4_tags["disk"][0][1] = tags[tag_name]
            elif tag_name in ("track", "track_total"):
                if mp4_tags.get("trkn") is None:
                    mp4_tags["trkn"] = [[0, 0]]
                if tag_name == "track":
                    mp4_tags["trkn"][0][0] = tags[tag_name]
                elif tag_name == "track_total":
                    mp4_tags["trkn"][0][1] = tags[tag_name]
            if (
                MP4_TAGS_MAP.get(tag_name) is not None
                and tags.get(tag_name) is not None
            ):
                mp4_tags[MP4_TAGS_MAP[tag_name]] = [tags[tag_name]]
        if "cover" not in self.exclude_tags and self.cover_format != CoverFormat.RAW:
            mp4_tags["covr"] = [
                MP4Cover(
                    self.get_url_response_bytes(cover_url),
                    imageformat=(
                        MP4Cover.FORMAT_JPEG
                        if self.cover_format == CoverFormat.JPG
                        else MP4Cover.FORMAT_PNG
                    ),
                )
            ]
        mp4 = MP4(path)
        mp4.clear()
        mp4.update(mp4_tags)
        mp4.save()

    def move_to_output_path(
        self,
        remuxed_path: Path,
        final_path: Path,
    ):
        final_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(remuxed_path, final_path)

    @functools.lru_cache()
    def save_cover(self, cover_path: Path, cover_url: str):
        cover_path.write_bytes(self.get_url_response_bytes(cover_url))

    def cleanup_temp_path(self):
        shutil.rmtree(self.temp_path)
