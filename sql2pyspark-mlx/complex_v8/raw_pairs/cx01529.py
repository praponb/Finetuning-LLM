from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("sku_id").isNotNull()))
    .select("qty", F.col("unit_price").alias("value"))
)

move_rows_2 = df_stock_moves.select("qty", F.col("unit_price").alias("value"))

result = (
    move_rows
    .union(move_rows_2)
    .orderBy("value")
)
