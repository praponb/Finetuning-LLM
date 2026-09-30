from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & ((F.col("order_id") == 23) | (F.col("cost") >= 67)))
    .groupBy(
        F.when(F.col("cost") >= 81, "high").when(F.col("cost") >= 35, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .filter(F.col("delivered_count") >= 3)
)
