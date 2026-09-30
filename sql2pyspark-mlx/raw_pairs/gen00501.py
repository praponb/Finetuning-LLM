from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["DHL", "UPS", "Kerry"]) & (F.col("carrier") == "DHL"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        (F.datediff("shipped_date", "delivered_date") / 7).alias("weeks_delivered_date_to_shipped_date"),
    )
)
