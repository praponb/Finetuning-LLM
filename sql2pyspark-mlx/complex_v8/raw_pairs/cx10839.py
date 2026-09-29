from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_moves = (
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
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

with_prev = cleaned_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_move_type_running))

per_session = (
    sessionized
    .groupBy("move_type", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .groupBy("event_count", "burst_id")
    .pivot("move_type", ["INBOUND", "TRANSFER"])
    .agg(F.min("duration_min"))
)
