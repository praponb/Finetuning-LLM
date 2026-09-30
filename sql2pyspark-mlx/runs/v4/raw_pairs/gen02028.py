from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("promo_id", "id", "promo_id")
w5 = Window.partitionBy("store_id", "channel").orderBy(F.col("promo_id").asc_nulls_first())

with_window = (
    df_orders
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    with_window
    .select(
        "*",
        F.corr("amount", "shipping_fee").over(w).alias("corr_amount_shipping_fee"),
        F.corr("discount", "amount").over(w2).alias("corr_discount_amount"),
        F.sum("shipping_fee").over(w3).alias("cumulative_shipping_fee"),
        F.ntile(10).over(w4).alias("promo_id_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.cume_dist().over(w5).alias("promo_id_cume_dist"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
    )
)

matched_keys = (
    with_windows
    .select("store_id")
    .intersectAll(
        df_orders
        .filter(F.col("channel").isNotNull())
        .select("store_id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("store_id")
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
        F.round(F.sum("corr_discount_amount") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("corr_discount_amount_per_vip"),
        F.percentile_approx("cumulative_shipping_fee", 0.95).alias("cumulative_shipping_fee_p95"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
    )
    .orderBy(F.col("cumulative_shipping_fee_p95").desc())
)
