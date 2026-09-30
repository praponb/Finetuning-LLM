from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-04-01", "2025-05-18"))
    .select(
        "order_id",
        "cost",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
)
