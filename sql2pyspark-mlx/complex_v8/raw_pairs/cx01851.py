from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_release_date_ru = Window.orderBy(F.col("track_release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_track_release_date = Window.orderBy(F.col("track_release_date").desc(), "id")

cleaned = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.device_source_label",
        "s.played_at_label",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("l.username").alias("listener_username"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "track_id",
        "ms_played",
        "played_at_label",
        "track_is_explicit",
        "listener_username",
        "listener_birth_year",
        F.last("ms_played", ignorenulls=True).over(w_all_by_track_release_date_ru).alias("last_known_ms_played"),
        F.rank().over(w_all_by_track_release_date).alias("track_release_date_rank"),
    )
)

enriched_track = (
    windowed.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .filter(~F.col("t.is_explicit"))
    .select(
        "s.*",
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit_2"),
        F.col("t.artist_id").alias("track_artist_id"),
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_parent_2 = (
    enriched_track.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.track_artist_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played",
        "s.played_at_label",
        "s.track_is_explicit",
        "s.track_release_date_rank",
        "s.track_moods",
        "s.track_is_explicit_2",
        "s.track_artist_id",
        "s.artist_is_verified",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

combined_details = (
    with_parent_2.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.track_artist_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.track_moods",
        "s.track_is_explicit_2",
        "s.track_artist_id",
        "s.artist_debut_year",
        F.col("t.title").alias("track_title"),
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

enriched = (
    combined_details
    .select(
        "*",
        F.upper(F.trim(F.col("artist_genre"))).alias("artist_genre_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("artist_genre")), " ", "_")).alias("artist_genre_slug"),
        F.when(F.col("artist_genre").isin("HipHop", "Jazz"), "group_a").when(F.col("artist_genre") == "Classical", "group_b").otherwise("other").alias("artist_genre_group"),
    )
)

result = (
    enriched
    .select(
        "id",
        "track_id",
        "track_moods",
        "track_is_explicit_2",
        "track_artist_id",
        "artist_debut_year",
        "track_title",
        "artist_name",
        "artist_genre_clean",
        "artist_genre_slug",
        "artist_genre_group",
    )
)
