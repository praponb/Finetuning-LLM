from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNull() & (F.array_contains("tags", "eco") | (F.col("supplier_id") == 13)))
    .groupBy("brand")
    .agg(F.count("*").alias("product_count"))
)
