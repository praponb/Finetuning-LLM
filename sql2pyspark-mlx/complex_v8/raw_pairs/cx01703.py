from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("operator") != "op_bee")
    .select(F.col("warehouse_id").alias("qty"))
)

sku_side = (
    df_skus
    .filter((F.array_contains(F.col("labels"), "hazmat")) & (F.col("weight_kg") <= 21.0))
    .select("id")
)

result = (
    move_side
    .intersectAll(sku_side)
    .orderBy("qty")
)
