from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

with_prev = valid.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    per_session
    .crossJoin(per_session.agg(F.avg("session_id").alias("overall_avg_session_id")))
    .filter(F.col("session_id") > F.col("overall_avg_session_id"))
    .drop("overall_avg_session_id")
)

grouped = (
    with_activity
    .groupBy(
        F.date_trunc("month", "session_start").alias("session_start_month"),
        "session_id",
    )
    .agg(
        F.round(F.sum("duration_min") / F.nullif(F.sum("n_events"), F.lit(0)), 3).alias("duration_min_to_n_events_ratio"),
        F.round(100 * F.count(F.when(~F.col("operator").isin("op_eve", "op_bee"), 1)) / F.count("*"), 1).alias("pct_operator_not_in_op_eve_op_be"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    grouped
    .filter(F.col("pct_operator_not_in_op_eve_op_be") < 55.0)
    .select(
        "session_start_month",
        "session_id",
        "pct_operator_not_in_op_eve_op_be",
        "distinct_operators",
        "avg_session_id",
        "n_rows",
    )
    .orderBy("session_start_month")
)
