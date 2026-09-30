from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm_minus_duration_sec").desc(), F.col("id").desc())

valid = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 7) & (F.col("duration_sec") > 142))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

scored = (
    valid
    .withColumn("title_ref_slug", F.lower(F.regexp_replace(F.trim(F.col("title_ref")), " ", "_")))
)

derived = (
    scored
    .select(
        "*",
        (F.floor(F.col("bpm_minus_duration_sec") / 100) * 100).alias("bpm_minus_duration_sec_bucket"),
        F.length(F.trim(F.col("title_ref"))).alias("title_ref_len"),
    )
)

latest_per_key = (
    derived
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

line_level = (
    latest_per_key.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.bpm_minus_duration_sec",
        "s.title_ref",
        "s.bpm_minus_duration_sec_bucket",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

tagged = (
    line_level
    .select(
        "*",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        F.when(F.col("play_is_skipped"), 1).otherwise(0).alias("play_is_skipped_flag"),
        F.array_contains(F.col("moods"), "workout").alias("has_workout"),
    )
)

result = (
    tagged
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        "bpm_minus_duration_sec",
        "bpm_minus_duration_sec_bucket",
        "play_id",
        "play_played_at",
        "play_is_skipped",
        "play_ms_played",
        "title_prefix",
        "play_is_skipped_flag",
        "has_workout",
    )
    .orderBy(F.col("play_played_at").desc(), "bpm")
)
