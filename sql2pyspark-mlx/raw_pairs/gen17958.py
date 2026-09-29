from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date_minus_14d"), "channel")
w_recent = Window.orderBy(F.col("discount").desc_nulls_first())
w3 = Window.partitionBy("store_id")

cleaned = (
    df_orders
    .filter(~F.col("channel").eqNullSafe(F.lit("store")))
    .select(
        "*",
        F.date_sub("order_date", 14).alias("order_date_minus_14d"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

kept_keys = (
    cleaned
    .select("customer_id", "status")
    .exceptAll(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "status")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .filter(F.col("channel").isin(["store", "mobile"]))
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "order_date_minus_14d",
        "channel_filled",
        F.count("shipping_fee").over(w).alias("order_date_minus_14d_month_count_shipping_fee"),
        F.cume_dist().over(w_recent).alias("discount_cume_dist"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") < F.col("s.order_date")), "left_anti")
)

kept_keys_2 = (
    not_flagged
    .select("customer_id", "id")
    .subtract(
        df_orders
        .filter(F.col("amount") >= 1380)
        .select("customer_id", "id")
    )
)

reconciled = (
    not_flagged
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    reconciled
    .withColumn("order_date_minus_14d_month_count_shipping_fee_tier", F.when(F.col("order_date_minus_14d_month_count_shipping_fee") >= 16, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "order_date"))
    .cube("order_date_minus_14d_month_count_shipping_fee_tier", "year_start")
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("shipping_fee_per_store"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    daily
    .select(
        "order_date_minus_14d_month_count_shipping_fee_tier",
        "discount_stddev",
        "channel_list",
    )
    .orderBy("discount_stddev")
)
