from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter((F.col("unit_cost") <= 251.2) & (F.col("sku_code").isNotNull()))
    .select("id")
)

sku_rows = df_skus.select("id")

result = (
    sku_side
    .subtract(sku_rows)
    .orderBy("id")
)
