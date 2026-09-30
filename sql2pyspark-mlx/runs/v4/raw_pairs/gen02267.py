from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("shipped_date") <= "2023-10-01"))
    .select(
        "*",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

filtered = (
    valid
    .filter(F.col("order_id").isin([14, 27, 29]))
    .select(
        "*",
        F.length("carrier").alias("carrier_length"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.amount"))
)

result = joined.select("id", "order_id", "shipped_date")
