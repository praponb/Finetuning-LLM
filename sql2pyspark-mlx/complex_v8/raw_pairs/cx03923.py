from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_year_burst_id_by = Window.partitionBy(F.year("session_end"), "burst_id").orderBy("event_count", "operator", "burst_id")
w_all_by_session_qty_full = Window.orderBy("session_qty", "operator", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_session_start_month = Window.partitionBy(F.date_trunc("month", "session_start"))
w_all_by_session_qty = Window.orderBy("session_qty", "operator", "burst_id")

filtered_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

with_prev = filtered_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_operator_running))

sessions = (
    sessionized
    .groupBy("operator", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    sessions
    .select(
        "operator",
        "burst_id",
        "session_start",
        "event_count",
        "session_qty",
        "duration_min",
        F.lead("burst_id", 2).over(w_session_end_year_burst_id_by).alias("next2_burst_id"),
        F.first("session_qty").over(w_all_by_session_qty_full).alias("first_session_qty"),
        F.round(F.avg("event_count").over(w_session_start_month), 2).alias("session_start_month_avg_event_count"),
        F.lead("session_qty").over(w_all_by_session_qty).alias("next_session_qty"),
        F.round(F.col("duration_min") / F.nullif(F.sum("duration_min").over(w_session_start_month), F.lit(0)), 4).alias("duration_min_share_of_session_start_month"),
    )
)

result = (
    with_trends
    .filter(F.col("next_session_qty") < 1100)
    .select(
        "operator",
        "burst_id",
        "session_start",
        "event_count",
        "session_qty",
        "duration_min",
        "next2_burst_id",
        "first_session_qty",
        "session_start_month_avg_event_count",
        "next_session_qty",
        "duration_min_share_of_session_start_month",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_start")).alias("days_since_session_start"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
    .orderBy("operator", F.col("session_qty").desc_nulls_first())
)
