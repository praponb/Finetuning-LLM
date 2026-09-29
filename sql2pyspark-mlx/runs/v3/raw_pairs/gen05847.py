from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") > "2023-04-12") & (F.col("delivered_date") >= "2025-01-17") & (F.year("delivered_date") == 2024))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "shipped_date",
        F.substring("status", 1, 2).alias("status_prefix"),
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
    .orderBy(F.col("id").desc(), "shipped_date")
)
