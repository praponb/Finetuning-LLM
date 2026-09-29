from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter(F.col("unit_cost") <= 277.0)
    .select(
        F.col("category").alias("name"),
        F.col("id").alias("key_id"),
        F.col("unit_cost").alias("metric"),
    )
)

move_side = df_stock_moves.select("operator", "sku_id", "unit_price")

result = (
    sku_side
    .union(move_side)
    .distinct()
)
