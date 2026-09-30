from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("is_active"))

customers_stats = (
    recent_customers
    .groupBy("country", "segment")
    .agg(
        F.avg("age").alias("avg_age"),
        F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"),
    )
)

result = (
    customers_stats
    .filter(F.col("avg_age") > 47)
    .select("country", "segment", "jp_count")
)
