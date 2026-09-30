from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-04-08").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        (F.datediff(F.current_date(), "order_date") / 7).alias("weeks_since_order_date"),
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .select("channel", "store_id")
    .subtract(
        df_orders
        .filter(F.col("discount").isNotNull())
        .select("channel", "store_id")
    )
)
