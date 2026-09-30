from pyspark.sql import functions as F

sku_rows = df_skus.select(F.col("id").alias("ref_id"))

sku_side = (
    df_skus
    .filter((F.col("unit_cost") > 134.3) & (~F.array_contains(F.col("labels"), "fragile")))
    .select("id")
)

result = sku_rows.subtract(sku_side)
