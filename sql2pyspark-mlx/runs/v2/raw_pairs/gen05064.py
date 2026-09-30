from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 65)
    .select(
        "shipped_date",
        "carrier",
        "order_id",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.add_months("shipped_date", 3).alias("shipped_date_plus_months"),
    )
)
