from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())

recent_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("played_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

latest_per_key = (
    recent_plays
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest_per_key
    .filter(F.col("track_id").isNotNull())
    .select(
        "*",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.abs(F.col("ms_played") - 180879).alias("ms_played_distance"),
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
    )
)

result = (
    enriched
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "played_at_date",
        "ms_played_distance",
        "device_prefix",
    )
)
