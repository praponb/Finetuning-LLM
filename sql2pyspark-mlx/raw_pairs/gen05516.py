from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id") == 27), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(store|partner)$"), "GROUP_B").otherwise("CORE").alias("channel_zone"),
    )
    .orderBy(F.col("customer_id").desc(), "amount")
)
