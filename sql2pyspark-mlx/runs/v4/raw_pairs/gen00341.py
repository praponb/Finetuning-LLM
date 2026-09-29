from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-07-25"))) & F.col("order_id").isNotNull() & (F.col("shipped_date") <= "2025-04-01"))
    .select(
        "*",
        F.date_sub("shipped_date", 74).alias("shipped_date_minus_74d"),
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

result = (
    valid
    .groupBy("status")
    .pivot("carrier", ["DHL", "UPS"])
    .agg(F.sum("cost"))
    .select("status", F.col("DHL").alias("dhl"), F.col("UPS").alias("ups"))
)
