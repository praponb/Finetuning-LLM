from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter(~F.col("category").isin("Food", "Apparel"))
    .select("id", F.col("unit_cost").alias("amount"))
)

move_rows = df_stock_moves.select(F.col("qty").alias("id"), F.col("unit_price").alias("amount"))

result = (
    sku_side
    .unionByName(move_rows)
    .distinct()
    .orderBy(F.col("amount").desc(), "id")
)
