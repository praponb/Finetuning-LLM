from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("last_restock") < "2024-08-01") & (F.col("quantity") < 116))
)

summary = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 221, "high").when(F.col("quantity") >= 60, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("row_count"),
    )
)

result = summary.filter(F.col("row_count") >= 8)
