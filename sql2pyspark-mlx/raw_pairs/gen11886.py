from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-08-22"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.round((F.col("shipping_fee") / F.col("amount")), 2).alias("amount_with_shipping"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.region"))
)
