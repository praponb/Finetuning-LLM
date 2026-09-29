from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("cost") <= 84) & (F.year("shipped_date") == 2023)) | F.col("order_id").isin([20, 28]))
    .select(
        "carrier",
        "order_id",
        "cost",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_date_label"),
    )
    .orderBy("delivered_date_label")
)
