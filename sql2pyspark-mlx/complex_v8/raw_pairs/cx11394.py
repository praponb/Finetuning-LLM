from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter(F.col("category") == "Toys")
    .select(F.col("id").alias("ref_id"))
)

sku_rows = df_skus.select("id")

result = sku_side.subtract(sku_rows)
