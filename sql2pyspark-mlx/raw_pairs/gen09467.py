from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("stddev_revenue") >= 41)
    .select("product_id", "sale_quarter", "row_count", "stddev_revenue")
)
