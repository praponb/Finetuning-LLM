from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("total_revenue") >= 10000)
    .select("store_id", "sale_month", "max_revenue", "total_revenue")
    .orderBy(F.col("store_id").desc(), "sale_month")
)
