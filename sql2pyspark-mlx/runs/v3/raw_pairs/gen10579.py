from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.when(F.col("revenue") >= 3600, "high").when(F.col("revenue") >= 1000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
    .filter(F.col("stddev_units") >= 23)
    .select("store_id", "revenue_bucket", "stddev_units")
    .orderBy(F.col("stddev_units").desc())
)
