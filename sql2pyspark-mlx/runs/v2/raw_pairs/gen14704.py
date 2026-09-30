from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "cost",
        "shipped_date",
        "carrier",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.length("carrier").alias("carrier_length"),
    )
    .orderBy("carrier_length")
)
