from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") <= "2024-10-01") & (F.col("cost") > 86))
    .select(
        "id",
        "carrier",
        "delivered_date",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
    )
    .orderBy(
        "carrier_status_key",
        F.col("weeks_shipped_date_to_delivered_date").desc(),
        "id",
    )
    .limit(50)
)
