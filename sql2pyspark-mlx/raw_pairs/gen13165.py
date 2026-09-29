from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([22, 24]))
    .groupBy(
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
        "status",
        "carrier",
    )
    .agg(
        F.min("shipped_date").alias("min_shipped_date"),
        F.max("delivered_date").alias("latest_delivered_date"),
    )
)
