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
        F.when(F.col("status").isin(["PENDING", "RETURNED"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.status",
        "src.channel",
        "src.status_group",
        "src.channel_clean",
        F.col("p.sqft"),
        F.col("p.region"),
        F.col("p.opened_date"),
    )
)
