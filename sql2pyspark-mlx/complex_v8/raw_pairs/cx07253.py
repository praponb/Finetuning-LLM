from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(F.col("sku_id").alias("key_id"), "unit_price", "moved_at")
)

move_rows_2 = df_stock_moves.select(F.col("id").alias("key_id"), "unit_price", "moved_at")

result = (
    move_rows
    .unionByName(move_rows_2)
    .orderBy("key_id", "unit_price")
)
