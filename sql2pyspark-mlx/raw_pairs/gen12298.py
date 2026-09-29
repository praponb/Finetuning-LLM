from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
)
