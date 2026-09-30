from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        F.upper("channel").alias("channel_upper"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    base
    .groupBy("customer_id")
    .pivot("status", ["PENDING", "COMPLETED"])
    .agg(F.max("shipping_fee"))
)
