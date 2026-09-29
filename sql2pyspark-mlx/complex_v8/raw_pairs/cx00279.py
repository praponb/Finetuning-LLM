from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter(F.col("unit_price") < 290.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

with_prev = recent_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_operator_running))

sessions = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .filter(F.col("n_events") < 5)
    .select(
        "operator",
        "session_id",
        "n_events",
        "duration_min",
    )
    .orderBy(F.col("operator").desc(), "duration_min", "session_id")
    .limit(5)
)
