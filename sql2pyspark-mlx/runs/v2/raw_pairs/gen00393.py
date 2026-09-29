from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2024-03-11")
    .select(
        "id",
        "delivered_date",
        "shipped_date",
        "order_id",
        F.date_trunc("year", "shipped_date").alias("shipped_date_period"),
    )
)
