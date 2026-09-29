from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(((F.col("sale_date") > "2023-08-01") & (F.col("sale_date") <= "2023-10-27")) | (F.col("sale_date") >= "2025-04-01"))
)

agg = (
    valid_store_sales
    .groupBy("store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    agg
    .filter(F.col("max_revenue") >= 2300)
    .select("store_id", "max_revenue", "stddev_revenue")
    .orderBy("store_id")
)
