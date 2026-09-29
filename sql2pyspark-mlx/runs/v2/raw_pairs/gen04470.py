from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("shipping_fee") >= 35) & (F.col("discount") > 0.14))
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
        F.round((F.col("amount") * 0.85), 2).alias("amount_adjusted"),
    )
)
