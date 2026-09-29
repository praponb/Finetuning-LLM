from pyspark.sql import functions as F

kept_keys = (
    df_orders
    .select("status")
    .intersect(
        df_orders
        .filter(F.col("customer_id") == 31)
        .select("status")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
    )
    .orderBy(F.col("discount").desc_nulls_first())
)
