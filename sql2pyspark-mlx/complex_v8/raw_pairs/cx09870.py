from pyspark.sql import functions as F

move_side = df_stock_moves.select("warehouse_id", "unit_price", F.lit("move").alias("origin"))

sku_rows = (
    df_skus
    .select(
        F.col("id").alias("warehouse_id"),
        F.col("unit_cost").alias("unit_price"),
        F.lit("sku").alias("origin"),
    )
)

result = (
    move_side
    .unionByName(sku_rows)
    .orderBy("unit_price")
)
