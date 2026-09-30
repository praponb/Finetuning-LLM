from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("device").orderBy("device_len", "id")

valid_plays = (
    df_plays
    .filter(F.col("device").isin("speaker", "phone", "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

without_matches = (
    valid_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.is_explicit")) & (F.array_contains(F.col("t.moods"), "workout")), "left_semi")
)

latest_per_key = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

qualified = (
    latest_per_key
    .crossJoin(latest_per_key.agg(F.avg("ms_played").alias("overall_avg_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_avg_ms_played"))
    .drop("overall_avg_ms_played")
)

exploded_detail = (
    qualified.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_or_unknown",
        "s.source_clean",
        "s.device_len",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "listener_id",
        "ms_played",
        "source",
        "device_or_unknown",
        "source_clean",
        "device_len",
        "play_id",
        "play_is_skipped",
        "play_source",
        "play_device",
    )
    .orderBy(
        "play_source",
        F.col("play_device").desc(),
        "id",
        "play_id",
    )
    .limit(10)
)
