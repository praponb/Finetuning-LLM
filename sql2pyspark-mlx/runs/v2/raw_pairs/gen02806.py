from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 8, 9]))
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"), "product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.max("units").alias("max_units"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("avg_revenue") >= 79)
    .orderBy("sale_quarter", "store_id", "product_id")
    .limit(5)
)
