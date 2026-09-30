from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 77, "high").when(F.col("cost") >= 28, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
    )
    .filter(F.col("pct_fedex") >= 28)
    .select("cost_bucket", "pct_fedex", "in_transit_count")
    .orderBy(F.col("pct_fedex").desc(), "cost_bucket")
)
