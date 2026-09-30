from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "status",
        "carrier",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.date_add("shipped_date", 84).alias("shipped_date_plus_84d"),
    )
    .orderBy(F.col("shipped_date_month_end").desc())
)
