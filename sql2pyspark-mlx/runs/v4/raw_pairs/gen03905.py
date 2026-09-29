from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 75, "high").when(F.col("cost") >= 43, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.min("shipped_date").alias("min_shipped_date"),
    )
)
