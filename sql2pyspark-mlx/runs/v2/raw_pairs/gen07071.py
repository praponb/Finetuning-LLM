from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5]))
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
    .filter(F.col("max_revenue") >= 3200)
    .orderBy("total_revenue", F.col("sale_month").desc())
    .limit(1)
)
