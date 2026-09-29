from pyspark.sql import functions as F

sku_rows = df_skus.select(F.col("id").alias("ref_id"))

sku_rows_2 = (
    df_skus
    .filter((F.col("category").isNotNull()) & (F.col("weight_kg") < 18.5))
    .select(F.col("id").alias("ref_id"))
)

result = sku_rows.intersect(sku_rows_2)
