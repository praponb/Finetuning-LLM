from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_play_is_skipped = Window.partitionBy("artist_id", "play_is_skipped")
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_distance = Window.orderBy("duration_sec_distance", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_distance_2 = Window.orderBy("duration_sec_distance", "id", "play_id")

filtered = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("S%")) | (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("duration_sec") - 312).alias("duration_sec_distance"),
    )
)

exploded_detail = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.duration_sec_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

metrics = (
    exploded_detail
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "play_id",
        "play_is_skipped",
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_play_is_skipped), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_play_is_skipped"),
        F.last("bpm").over(w_all_by_duration_sec_full).alias("last_bpm"),
        F.last("play_played_at", ignorenulls=True).over(w_all_by_duration_sec_distance).alias("last_known_play_played_at"),
        F.row_number().over(w_all_by_duration_sec_distance_2).alias("seq_no"),
    )
)

top_rows = (
    metrics
    .filter(F.col("seq_no") <= 3)
    .drop("seq_no")
)

with_parent = (
    top_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.play_id",
        "s.play_is_skipped",
        "s.duration_sec_share_of_artist_id_play_is_skipped",
        "s.last_known_play_played_at",
        "a.artist_name",
    )
)

annotated = (
    with_parent
    .filter(F.upper(F.trim(F.col("artist_name"))).like("S%"))
    .select(
        "*",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("play_id").cast("string")).alias("artist_name_ref"),
    )
)

result = (
    annotated
    .select(
        "id",
        "play_id",
        "play_is_skipped",
        "duration_sec_share_of_artist_id_play_is_skipped",
        "last_known_play_played_at",
        "artist_name",
        "artist_name_ref",
        F.substring(F.trim(F.col("artist_name_ref")), 1, 4).alias("artist_name_ref_prefix"),
    )
    .orderBy(F.col("id").desc())
)
