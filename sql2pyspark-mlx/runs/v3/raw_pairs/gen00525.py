from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2025-03-18")
    .select(
        "order_id",
        "shipped_date",
        F.date_trunc("year", "delivered_date").alias("delivered_date_period"),
    )
)
