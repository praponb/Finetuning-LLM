from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Kerry") & ((F.col("delivered_date") <= "2025-03-01") | (F.col("status") == "LOST")))
    .select(
        "delivered_date",
        "status",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.date_trunc("YEAR", "shipped_date").alias("shipped_date_period"),
    )
    .orderBy("shipped_date_month_end")
)
