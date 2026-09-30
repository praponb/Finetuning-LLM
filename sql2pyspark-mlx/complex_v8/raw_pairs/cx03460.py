from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end_ties = Window.orderBy("session_end")
w_session_start_to_date_by_ses = Window.partitionBy(F.to_date("session_start")).orderBy(F.col("session_start").asc_nulls_last(), "operator", "burst_id")
w_all_by_session_end_running = Window.orderBy("session_end", "operator", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end = Window.orderBy("session_end", "operator", "burst_id")
w_events = Window.partitionBy("events")

valid_moves = (
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
    )
)

with_prev = valid_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_operator_running))

per_session = (
    sessionized
    .groupBy("operator", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    per_session
    .select(
        "operator",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        F.round(F.percent_rank().over(w_all_by_session_end_ties), 3).alias("session_end_percent_rank"),
        (F.col("events") - F.lag("events").over(w_session_start_to_date_by_ses)).alias("events_change"),
        F.sum("burst_id").over(w_all_by_session_end_running).alias("running_burst_id"),
        F.round(F.cume_dist().over(w_all_by_session_end_ties), 3).alias("session_end_cume_dist"),
        F.lead("burst_id", 1, 0).over(w_all_by_session_end).alias("next_burst_id"),
        F.count(F.lit(1)).over(w_events).alias("rows_in_events"),
    )
)

result = (
    with_trends
    .select(
        "operator",
        "burst_id",
        "session_end",
        "session_end_percent_rank",
        "events_change",
        "running_burst_id",
        "session_end_cume_dist",
        "rows_in_events",
        F.date_trunc("day", F.col("session_end")).alias("session_end_day"),
    )
    .orderBy("events_change", "session_end_day")
)
