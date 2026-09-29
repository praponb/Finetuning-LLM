from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_moves = df_stock_moves.filter(F.col("warehouse_id") <= 5)

with_prev = filtered_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_sku_id_running))

result = (
    sessionized
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "prev_moved_at",
        "new_session_flag",
    )
    .orderBy(F.col("new_session_flag").desc(), "id")
    .limit(20)
)
