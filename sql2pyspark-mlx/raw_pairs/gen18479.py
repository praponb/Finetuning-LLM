from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("shipping_fee").alias("avg_order_shipping_fee"))

valid = (
    df_orders
    .filter(~F.col("channel").eqNullSafe(F.lit("mobile")))
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
        F.length("status").alias("status_length"),
    )
)

cleaned = (
    valid
    .filter(F.col("channel").isNotNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("shipping_fee_band", F.when(F.col("shipping_fee") >= 32, "High").when(F.col("shipping_fee") >= 10, "Medium").otherwise("Low"))
)

matched_keys = (
    cleaned
    .select("status", "channel")
    .intersectAll(
        df_orders
        .filter(F.col("channel") == "store")
        .select("status", "channel")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([16, 23, 36, 37])), "left_anti")
)

result = (
    eligible
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "status_length",
        "status_title",
        F.col("avg_order_shipping_fee"),
    )
)
