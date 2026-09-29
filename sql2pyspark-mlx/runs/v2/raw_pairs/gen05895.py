from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.count("cost").alias("cost_count"),
    )
)
