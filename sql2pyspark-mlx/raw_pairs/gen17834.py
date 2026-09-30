from pyspark.sql import functions as F

shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

base = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-04-22").cast("date"))
    .withColumn("discount_rounded", F.round("discount", 2))
)

eligible = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity").between(6, 8)), "left_anti")
)

eligible_orders = (
    eligible
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.to_date(F.col("c.paid_at")) >= "2024-10-02"), "left_semi")
)

cleaned = (
    eligible_orders
    .filter(F.col("customer_id").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        "discount_rounded",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
        F.last_day("order_date").alias("order_date_month_end"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "channel",
        "amount",
        "shipping_fee",
        "discount_rounded",
        "amount_with_shipping",
        "order_date_month_end",
        "resolved_channel",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("amount").desc_nulls_first(), "order_date_month_end")
)
