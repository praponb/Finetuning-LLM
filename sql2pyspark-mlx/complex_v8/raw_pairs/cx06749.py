from pyspark.sql import functions as F

sku_side = df_skus.select("category")

sku_side_2 = (
    df_skus
    .filter((F.col("unit_cost") <= 306.4) & (F.col("is_active")))
    .select("category")
)

result = (
    sku_side
    .exceptAll(sku_side_2)
    .orderBy("category")
)
