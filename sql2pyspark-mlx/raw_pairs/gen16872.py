from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2025-07-01")
    .select(
        "carrier",
        (F.datediff("shipped_date", "delivered_date") / 7).alias("weeks_delivered_date_to_shipped_date"),
    )
)
