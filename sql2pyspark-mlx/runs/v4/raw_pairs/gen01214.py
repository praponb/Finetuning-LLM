from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.round((F.col("discount") * 0.9), 2).alias("discount_adjusted"),
        F.when(F.col("channel").isin(["store", "web"]), "Tier 1").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.sqft"))
)
