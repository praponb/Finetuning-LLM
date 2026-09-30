from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
)
