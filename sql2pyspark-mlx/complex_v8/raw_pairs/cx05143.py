from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_all_by_played_at_month_runni = Window.orderBy("played_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 239955, "large").when(F.col("ms_played") >= 92277, "medium").otherwise("small").alias("ms_played_tier"),
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
    )
)

with_trends = (
    recent_plays
    .select(
        "*",
        F.min("ms_played").over(w_device).alias("device_min_ms_played"),
        F.size(F.collect_set("listener_id").over(w_device)).alias("distinct_listener_id_in_device"),
        F.sum("ms_played").over(w_device).alias("device_sum_ms_played"),
        F.max("ms_played").over(w_device).alias("device_max_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_all_by_played_at_month_runni).alias("last_known_ms_played"),
    )
)

annotated = (
    with_trends
    .select(
        "*",
        F.date_trunc("quarter", F.col("played_at_month")).alias("played_at_month_quarter"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at_month")).alias("days_since_played_at_month"),
    )
)

result = (
    annotated
    .select(
        "id",
        "track_id",
        "is_skipped",
        "ms_played_tier",
        "played_at_month",
        "device_min_ms_played",
        "distinct_listener_id_in_device",
        "device_max_ms_played",
        "played_at_month_quarter",
        "days_since_played_at_month",
        F.substring(F.trim(F.col("ms_played_tier")), 1, 4).alias("ms_played_tier_prefix"),
    )
    .orderBy("ms_played_tier")
)
