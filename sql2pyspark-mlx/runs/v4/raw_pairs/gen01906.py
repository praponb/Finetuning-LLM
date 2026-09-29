from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "DELIVERED"]) & (F.col("carrier") == "DHL"))
    .groupBy("status", F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
    .orderBy(F.col("status").desc())
)
