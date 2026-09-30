from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("channel")

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-13").cast("date")) & F.col("channel").isNotNull())
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
        F.date_add("order_date", 79).alias("order_date_plus_79d"),
        F.least("shipping_fee", "discount").alias("least_shipping_fee_discount"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

kept_keys = (
    filtered
    .select("store_id", "id")
    .intersectAll(
        df_orders
        .filter(F.year("order_date") == 2023)
        .select("store_id", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("store_id", "status")
    .intersectAll(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("store_id", "status")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") < F.col("s.order_date_plus_79d")), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

attributed = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.order_date_plus_79d").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

eligible = (
    attributed
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price") <= 240), "left_anti")
)

result = (
    eligible
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.segment").alias("segment"), F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.least_shipping_fee_discount")).alias("total_least_shipping_fee_discount"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
