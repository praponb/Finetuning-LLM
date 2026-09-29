from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.when(F.col("revenue") >= 3300, "high").when(F.col("revenue") >= 1600, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("*").alias("store_sale_count"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .filter(F.col("store_sale_count") > 4)
    .orderBy("stddev_units", "product_id", "revenue_bucket")
    .limit(1)
)
