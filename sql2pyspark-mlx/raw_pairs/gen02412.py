from pyspark.sql import functions as F

kept_keys = (
    df_orders
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(web|store)$"), "PRIORITY").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last(), "order_date")
)
