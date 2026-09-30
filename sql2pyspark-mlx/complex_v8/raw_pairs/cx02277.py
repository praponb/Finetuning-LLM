from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")
w_is_skipped_by_ms_played_max = Window.partitionBy("is_skipped").orderBy("ms_played_max_3", "id")

cleaned = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")) & (F.col("source") == "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.max("ms_played").over(w_device_listener_id_by_ms_pla).alias("ms_played_max_3"),
        F.lag("ms_played", 2).over(w_is_skipped_by_ms_played).alias("prev2_ms_played"),
    )
)

screened = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") >= 73625), "left_anti")
    .filter(F.col("prev2_ms_played") < 290754)
)

metrics = (
    screened
    .select(
        "*",
        F.lead("ms_played", 3).over(w_is_skipped_by_ms_played_max).alias("next3_ms_played"),
        F.round(F.cume_dist().over(w_is_skipped_by_ms_played_max), 3).alias("ms_played_max_3_cume_dist"),
    )
)

result = (
    metrics
    .groupBy("listener_id", "track_id")
    .pivot("source", ["search", "playlist", "album", "radio"])
    .agg(F.max("prev2_ms_played"))
    .orderBy("listener_id", "track_id")
)
