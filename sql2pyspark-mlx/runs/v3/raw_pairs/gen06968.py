from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 4100, "high").when(F.col("revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.max("units").alias("max_units"))
    .filter(F.col("max_units") < 12)
    .select("revenue_bucket", "max_units")
)
