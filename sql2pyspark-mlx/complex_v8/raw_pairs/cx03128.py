from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter(F.col("qty") > 93)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_prev = recent_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_move_type_running))

per_session = (
    sessionized
    .groupBy("move_type", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = per_session.withColumn("move_type_clean", F.lower(F.trim(F.col("move_type"))))

result = (
    annotated
    .filter(F.col("session_end") >= F.lit("2025-01-09 09:00:00").cast("timestamp"))
    .select(
        "move_type",
        "session_id",
        "session_end",
        "n_events",
        "session_unit_price",
        "duration_min",
        "move_type_clean",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean_2"),
    )
    .orderBy("duration_min", "session_id")
)
