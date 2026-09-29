from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy(F.col("max_cost").desc())
)
