from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("order_id") == 9) & F.col("order_id").isin([7, 25, 30])) | (F.col("status") == "LOST"))
    .select(
        "id",
        "cost",
        "carrier",
        "shipped_date",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
    )
    .orderBy(F.col("shipped_date").desc(), "id")
    .limit(10)
)
