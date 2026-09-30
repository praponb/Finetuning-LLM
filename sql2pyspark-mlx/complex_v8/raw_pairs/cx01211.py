from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter(F.col("unit_price") >= 60.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 330.9, "A").when(F.col("unit_price") >= 209.4, "B").otherwise("C").alias("unit_price_tier"),
    )
)

with_prev = valid_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_move_type_running))

result = (
    sessionized
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "is_new_session",
        "burst_id",
    )
    .orderBy(F.col("burst_id").asc(), "warehouse_id")
)
