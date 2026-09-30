from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee").desc_nulls_first())
w2 = Window.partitionBy("status")
w3 = Window.orderBy(F.col("amount_minus_discount").desc_nulls_last())
w_ordered = Window.partitionBy(F.date_trunc("week", "order_date"))

valid = (
    df_orders
    .filter(F.col("store_id").isin([15, 38]) & F.col("channel").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "channel", "status").alias("channel_status_key"),
        (F.col("amount") - F.col("discount")).alias("amount_minus_discount"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

kept_keys = (
    valid
    .select("channel", "customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("status").isin(["PENDING", "SHIPPED"]))
        .select("channel", "customer_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("customer_id").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("partner"))))
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
        "channel_status_key",
        "amount_minus_discount",
        "channel_clean",
        (F.col("shipping_fee") - F.col("amount")).alias("amount_with_shipping"),
        (F.col("shipping_fee") - F.col("amount_minus_discount")).alias("shipping_fee_minus_amount_minus_discount"),
    )
)

scored = (
    filtered
    .filter(F.col("amount_minus_discount") <= 20)
    .select(
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "channel_status_key",
        "amount_minus_discount",
        "channel_clean",
        "amount_with_shipping",
        "shipping_fee_minus_amount_minus_discount",
        F.cume_dist().over(w).alias("shipping_fee_cume_dist"),
        F.corr("shipping_fee", "amount").over(w2).alias("corr_shipping_fee_amount"),
    )
)

base = (
    scored
    .filter(F.col("discount") <= 0.18)
    .withColumn("amount_whole", F.ceil("amount"))
)

scored_orders = (
    base
    .select(
        "*",
        F.dense_rank().over(w3).alias("amount_minus_discount_dense_rank"),
        F.min("shipping_fee_cume_dist").over(w_ordered).alias("order_date_period_min_shipping_fee_cume_dist"),
    )
)

report = (
    scored_orders
    .select(
        "status",
        "amount",
        "channel_status_key",
        "amount_minus_discount",
        "channel_clean",
        "amount_with_shipping",
        "shipping_fee_cume_dist",
        "amount_whole",
        "amount_minus_discount_dense_rank",
        F.when(F.col("status").rlike("^(RETURNED|CANCELLED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(
        F.col("status_zone").desc_nulls_last(),
        F.col("amount_minus_discount_dense_rank").desc(),
    )
)

result = (
    report
    .select(
        "status",
        "amount",
        "channel_status_key",
        "amount_minus_discount",
        "channel_clean",
        "amount_with_shipping",
        "shipping_fee_cume_dist",
        "status_zone",
        F.when(F.col("status_zone").rlike("^(PRIORITY|GROUP_A)$"), "GROUP_B").when(F.col("status_zone").rlike("^(OTHER_ZONE)$"), "GROUP_A").otherwise("CORE").alias("status_zone_zone"),
    )
)
