from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", "carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
    .filter(F.col("pct_fedex") >= 72)
)
