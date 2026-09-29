from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("customer_id", "status")
    .intersect(
        df_orders
        .filter(F.col("status") == "SHIPPED")
        .select("customer_id", "status")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(partner|mobile)$"), "CORE").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc_nulls_first(), F.col("store_id").desc())
)
