from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2024-07-01") & ((F.col("shipped_date") <= "2025-06-12") | (F.col("cost") < 80)))
    .select(
        "id",
        "delivered_date",
        "carrier",
        "status",
        F.upper("carrier").alias("carrier_upper"),
        F.date_trunc("year", "shipped_date").alias("shipped_date_period"),
    )
    .orderBy("carrier", "id")
    .limit(3)
)
