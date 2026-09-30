from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([7, 8, 11]) & F.col("order_id").isNull())
    .select(
        "id",
        "delivered_date",
        "cost",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.upper("carrier").alias("carrier_upper"),
    )
    .orderBy(F.col("delivered_date").desc(), F.col("id").desc())
)
