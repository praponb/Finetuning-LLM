from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-03-27").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"))
)

result = (
    base
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.city") == "New York")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.status",
        "src.amount",
        "src.discount",
        F.col("d.opened_date"),
        F.col("d.sqft"),
    )
)
