from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") >= 3800) & F.col("product_id").isin([1, 5]))
    .groupBy(
        F.when(F.col("revenue") >= 4100, "high").when(F.col("revenue") >= 1400, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.min("units").alias("min_units"))
    .orderBy("revenue_bucket")
)
