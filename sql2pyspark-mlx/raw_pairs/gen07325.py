from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Kerry", "Flash"]) | (F.col("delivered_date") > "2025-03-01"))
    .select(
        "cost",
        "status",
        F.coalesce("carrier", F.lit("Unknown")).alias("carrier_filled"),
        F.date_trunc("week", "shipped_date").alias("shipped_date_period"),
    )
    .orderBy("status")
)
