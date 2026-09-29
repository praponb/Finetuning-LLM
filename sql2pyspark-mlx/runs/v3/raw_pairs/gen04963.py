from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 3500)
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") < 1500)
    .select("store_id", "sale_quarter", "last_sale_date", "avg_revenue")
    .orderBy(F.col("last_sale_date").desc())
)
