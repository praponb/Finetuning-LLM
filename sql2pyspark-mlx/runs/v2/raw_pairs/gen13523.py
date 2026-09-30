from pyspark.sql import functions as F

src = df_shipments.filter((F.col("carrier") == "Kerry") | (F.col("order_id") == 32))

shipments_stats = (
    src
    .groupBy(
        F.when(F.col("cost") >= 85, "high").when(F.col("cost") >= 43, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
)

result = (
    shipments_stats
    .filter(F.col("delivered_count") > 47)
    .select("cost_bucket", "delivered_count")
)
