from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 5, 6, 10]) & (F.col("units").isNotNull() | F.col("revenue").between(1600, 3800)))
    .groupBy(
        F.when(F.col("revenue") >= 3500, "high").when(F.col("revenue") >= 700, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.max("units").alias("max_units"))
)
