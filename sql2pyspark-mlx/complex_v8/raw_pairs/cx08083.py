from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter((F.col("qty") > 91) & (F.col("warehouse_id").isNotNull()))
    .select(F.col("qty").alias("ref_id"), "moved_at")
)

move_rows_2 = (
    df_stock_moves
    .filter(F.col("unit_price").between(119.5, 393.3))
    .select(F.col("sku_id").alias("ref_id"), "moved_at")
)

result = (
    move_rows
    .unionByName(move_rows_2)
    .orderBy("ref_id", "moved_at")
)
