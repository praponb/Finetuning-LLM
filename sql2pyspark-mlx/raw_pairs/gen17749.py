from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.count("*").alias("cnt"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .orderBy(F.col("latest_shipped_date").desc(), "pct_fedex")
)
