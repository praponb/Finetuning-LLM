from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("carrier") == "UPS")
    .withColumn("delivered_date_plus_months", F.add_months("delivered_date", 3))
)

shipments_stats = (
    base
    .groupBy(
        F.when(F.col("cost") >= 102, "high").when(F.col("cost") >= 45, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0)).alias("lost_count"))
)

result = shipments_stats
