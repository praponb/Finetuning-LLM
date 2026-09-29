from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_by_n_events_running = Window.partitionBy("burst_id").orderBy("n_events", "operator", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_events_burst_id = Window.partitionBy("n_events", "burst_id")
w_burst_id_by_n_events_prior_3 = Window.partitionBy("burst_id").orderBy("n_events", "operator", "burst_id").rowsBetween(-3, -1)

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
        "unit_price",
        F.abs(F.col("qty") - 230).alias("qty_distance"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_operator_running))

sessions = (
    sessionized
    .groupBy("operator", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    sessions
    .select(
        "*",
        F.count(F.lit(1)).over(w_burst_id_by_n_events_running).alias("running_count"),
        F.min("n_events").over(w_n_events_burst_id).alias("n_events_burst_id_min_n_events"),
        F.round(F.avg("n_events").over(w_burst_id_by_n_events_prior_3), 2).alias("n_events_avg_3_prior"),
    )
)

with_activity = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("running_count").alias("overall_avg_running_count")))
    .filter(F.col("running_count") > F.col("overall_avg_running_count"))
    .drop("overall_avg_running_count")
)

result = (
    with_activity
    .groupBy("session_start", "session_end")
    .pivot("operator", ["op_bee", "op_ann", "op_eve", "op_dew"])
    .agg(F.sum("n_events"))
    .orderBy("session_start", "session_end")
)
