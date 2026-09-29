from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

with_prev = filtered_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_sku_id_running))

result = (
    sessionized
    .filter(F.col("prev_moved_at") >= F.lit("2025-01-31 07:00:00").cast("timestamp"))
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "is_new_session",
        "burst_id",
    )
    .orderBy("move_type", F.col("unit_price").desc())
)
