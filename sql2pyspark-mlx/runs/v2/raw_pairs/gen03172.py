from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(28, 35))
    .groupBy(
        "store_id",
        F.when(F.col("units") >= 37, "high").when(F.col("units") >= 11, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") < 6)
    .select("store_id", "units_bucket", "last_sale_date", "revenue_count")
    .orderBy("store_id")
)
