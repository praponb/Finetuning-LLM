from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter((~F.col("is_active")) & (F.upper(F.trim(F.col("sku_code"))).like("A%")))
    .select("sku_code")
)

sku_side_2 = df_skus.select(F.col("category").alias("sku_code"))

result = (
    sku_side
    .subtract(sku_side_2)
    .orderBy("sku_code")
)
