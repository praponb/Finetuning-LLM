from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_device_by_played = Window.partitionBy("is_skipped", "device").orderBy("played_at")
w_device = Window.partitionBy("device")
w_all_by_played_at_percent_ran = Window.orderBy(F.col("played_at_percent_rank").desc(), "id").rowsBetween(-1, Window.currentRow)

scoped_plays = (
    df_plays
    .filter((F.col("source") != "radio") & (F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

with_windows = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.round(F.percent_rank().over(w_is_skipped_device_by_played), 3).alias("played_at_percent_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        F.sum("played_at_percent_rank").over(w_device).alias("device_sum_played_at_percent_rank"),
        F.round(F.col("played_at_percent_rank") / F.nullif(F.sum("played_at_percent_rank").over(w_device), F.lit(0)), 4).alias("played_at_percent_rank_share_of_device"),
        F.sum("played_at_percent_rank").over(w_all_by_played_at_percent_ran).alias("played_at_percent_rank_sum_2"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
    )
)

result = (
    metrics
    .select(
        "id",
        F.col("device_sum_played_at_percent_rank").alias("m_device_sum_played_at_percent_rank"),
        F.col("played_at_percent_rank_share_of_device").alias("played_at_percent_rank_share_of_device_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_device_sum_played_at_percent_rank", "played_at_percent_rank_share_of_device_v"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
