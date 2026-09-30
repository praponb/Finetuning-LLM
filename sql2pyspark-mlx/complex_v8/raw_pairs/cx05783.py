from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("play_device", "play_listener_id").orderBy("play_played_at", "id", "play_id")

base = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

detail_rows = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.release_date_week",
        "s.title_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

latest_per_key = (
    detail_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_activity_2 = (
    latest_per_key.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source").isNotNull()), "left_anti")
)

stats = (
    with_activity_2
    .filter(F.hour(F.col("release_date_week")).between(6, 12))
    .groupBy("artist_id")
    .agg(
        F.min("release_date_week").alias("first_release_date_week"),
        F.round(F.sum("bpm") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("bpm_to_duration_sec_ratio"),
        F.count(F.when(F.col("play_device").isin("phone", "tv", "speaker"), 1)).alias("n_play_device_in_phone_tv_spea"),
        F.min("play_played_at").alias("first_play_played_at"),
        F.round(F.sum("duration_sec") / F.nullif(F.sum("bpm"), F.lit(0)), 3).alias("duration_sec_to_bpm_ratio"),
    )
)

result = (
    stats
    .select(
        "artist_id",
        "bpm_to_duration_sec_ratio",
        "duration_sec_to_bpm_ratio",
        F.date_trunc("hour", F.col("first_play_played_at")).alias("first_play_played_at_hour"),
    )
    .orderBy(F.col("duration_sec_to_bpm_ratio").desc())
)
