from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter(F.col("operator") == "op_eve")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_operator_running))

session_summary = (
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

screened = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("event_count").alias("overall_avg_event_count")))
    .filter(F.col("event_count") > F.col("overall_avg_event_count"))
    .drop("overall_avg_event_count")
)

with_activity = (
    screened
    .crossJoin(screened.agg(F.max("session_qty").alias("overall_max_session_qty")))
    .filter(F.col("session_qty") < F.col("overall_max_session_qty"))
    .drop("overall_max_session_qty")
)

result = (
    with_activity
    .select(
        "operator",
        "session_end",
        "event_count",
        "session_qty",
        "duration_min",
    )
    .orderBy("session_end")
)
