from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") > "2025-04-01") & (F.col("amount") > 1430))
    .groupBy(
        F.when(F.col("discount") >= 0.34, "high").when(F.col("discount") >= 0.14, "mid").otherwise("low").alias("discount_bucket"),
        "status",
    )
    .agg(
        F.count("*").alias("num_orders"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
    .filter(F.col("completed_count") >= 72)
)
