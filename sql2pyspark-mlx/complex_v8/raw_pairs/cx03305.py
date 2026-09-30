from pyspark.sql import functions as F

move_rows = df_stock_moves.select("sku_id", "unit_price")

move_side = df_stock_moves.select(F.col("id").alias("sku_id"), "unit_price")

stacked = move_rows.unionByName(move_side)

result = (
    stacked
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy("sku_id")
)
