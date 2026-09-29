from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.count("*").alias("num_shipments"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .filter(F.col("num_shipments") >= 4)
)
