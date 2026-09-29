from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 35, "high").when(F.col("units") >= 15, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.max("revenue").alias("max_revenue"))
    .filter(F.col("max_revenue") > 2600)
    .select("units_bucket", "max_revenue")
    .orderBy("units_bucket")
    .limit(50)
)
