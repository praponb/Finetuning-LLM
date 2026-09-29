from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_session_start_to = Window.partitionBy("event_count", F.to_date("session_start"))
w_all_by_session_start = Window.orderBy("session_start", "operator", "session_id")
w_operator_by_duration_min = Window.partitionBy("operator").orderBy(F.col("duration_min").desc(), "operator", "session_id")
w_operator_session_end_month_b = Window.partitionBy("operator", F.date_trunc("month", "session_end")).orderBy("session_end", "operator", "session_id")

scoped = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

with_prev = scoped.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    per_session
    .select(
        "*",
        F.round(F.avg("event_count").over(w_event_count_session_start_to), 2).alias("event_count_session_start_to_date_avg_event_coun"),
        F.row_number().over(w_all_by_session_start).alias("seq_no"),
        F.lag("event_count", 2).over(w_operator_by_duration_min).alias("prev2_event_count"),
        (F.col("event_count") - F.lag("event_count").over(w_operator_session_end_month_b)).alias("event_count_change"),
    )
)

result = (
    metrics
    .select(
        "operator",
        "session_id",
        "session_start",
        "duration_min",
        "prev2_event_count",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)
