from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.col("duration_sec") > 164)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.when(F.col("bpm") >= 147, "large").when(F.col("bpm") >= 96, "medium").otherwise("small").alias("bpm_tier"),
    )
)

exploded_detail_plays = (
    base.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

filtered = (
    exploded_detail_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id") <= 19) & (F.month(F.col("p.played_at")).isin(1, 2)), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "play_listener_id",
        "play_source",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.initcap(F.trim(F.col("play_device"))).alias("play_device_clean"),
    )
    .orderBy(F.col("play_device_clean").desc(), F.col("id").desc_nulls_first())
)
