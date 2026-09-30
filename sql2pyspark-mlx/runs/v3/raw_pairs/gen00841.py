from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.min("revenue").alias("min_revenue"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
    .filter(F.col("revenue_range") < 25)
    .orderBy("revenue_range", "min_revenue", "store_id")
    .limit(20)
)
