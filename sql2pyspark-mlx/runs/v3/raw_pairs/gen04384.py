from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "delivered_date",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
    )
    .orderBy(F.col("days_shipped_date_to_delivered_date").desc())
)
