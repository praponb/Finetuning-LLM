from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 10]) & (F.col("store_id") == 3) & (F.col("revenue") < 1000))
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 33, "high").when(F.col("units") >= 13, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        F.count("units").alias("units_count"),
    )
    .orderBy("units_count")
)
