from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("unit_price").between(60.4, 298.9))
    .select(F.lit("move").alias("record_type"), "qty", "moved_at")
)

move_side_2 = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("operator").isin("op_chan", "op_eve")))
    .select(F.lit("move_2").alias("record_type"), "qty", "moved_at")
)

result = move_side.union(move_side_2)
