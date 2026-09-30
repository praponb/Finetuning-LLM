from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter(F.col("weight_kg") <= 14.6)
    .select(F.col("id").alias("qty"))
)

sku_side = df_skus.select("id")

result = (
    sku_rows
    .intersect(sku_side)
    .orderBy("qty")
)
