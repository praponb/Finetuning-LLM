from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("cost") > 79)

cost_bucket_summary = (
    recent_shipments
    .groupBy(
        F.when(F.col("cost") >= 84, "high").when(F.col("cost") >= 38, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = cost_bucket_summary
