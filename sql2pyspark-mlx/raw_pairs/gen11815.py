from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "IN_TRANSIT")
    .select(
        "id",
        "delivered_date",
        "carrier",
        "cost",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
    )
)
