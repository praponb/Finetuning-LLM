from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
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
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_operator_running))

session_summary = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("event_count").alias("overall_avg_event_count")))
    .filter(F.col("event_count") > F.col("overall_avg_event_count"))
    .drop("overall_avg_event_count")
)

scored = qualified.withColumn("session_start_date", F.to_date(F.col("session_start")))

result = (
    scored
    .select(
        "operator",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "session_qty",
        "duration_min",
        "session_start_date",
        F.date_format(F.col("session_start_date"), "yyyy-MM-dd").alias("session_start_date_label"),
    )
    .orderBy(F.col("session_id").desc(), F.col("event_count").asc_nulls_last())
)
