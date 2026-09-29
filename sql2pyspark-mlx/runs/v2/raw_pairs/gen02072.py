from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-05-01")
    .select(
        "order_id",
        "carrier",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.length("status").alias("status_length"),
    )
    .orderBy("carrier")
)
