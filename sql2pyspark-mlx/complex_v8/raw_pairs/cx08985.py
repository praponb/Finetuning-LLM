from pyspark.sql import functions as F

sku_side = df_skus.select(F.col("id").alias("qty"))

sku_side_2 = (
    df_skus
    .filter((F.col("weight_kg") >= 6.5) & (~F.col("is_active")))
    .select("id")
)

result = (
    sku_side
    .intersect(sku_side_2)
    .orderBy("qty")
)
