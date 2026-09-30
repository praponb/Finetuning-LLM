from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy(F.to_date("created_at"), "promo")
w3 = Window.partitionBy(F.year("created_at"))
w_grp = Window.partitionBy("promo").orderBy(F.col("price_per_qty").asc_nulls_last())
w5 = Window.partitionBy("promo").orderBy(F.col("qty").desc())
w_hist = Window.partitionBy("promo").orderBy(F.col("qty_max_30m").desc_nulls_first())
w7 = Window.partitionBy("promo")

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

cleaned = (
    lines
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & (F.col("created_at") >= "2024-05-23"))
    .withColumn("price_per_qty", F.round((F.col("price") / F.col("qty")), 2))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("NONE")))
)

remaining_keys = (
    cleaned
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "qty",
        "promo",
        "price_per_qty",
        "resolved_promo",
        F.avg("price_per_qty").over(w_ordered).alias("price_per_qty_avg_30m"),
        F.max("qty").over(w_ordered).alias("qty_max_30m"),
        F.stddev_samp("qty").over(w2).alias("created_at_to_date_stddev_samp_qty"),
        F.min("price_per_qty").over(w3).alias("created_at_year_min_price_per_qty"),
        F.percent_rank().over(w_grp).alias("price_per_qty_percent_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("created_at_to_date_stddev_samp_qty") >= 2)
    .select(
        "qty",
        "promo",
        "price_per_qty",
        "resolved_promo",
        "price_per_qty_avg_30m",
        "qty_max_30m",
        "created_at_to_date_stddev_samp_qty",
        "price_per_qty_percent_rank",
        F.coalesce("promo", F.lit("Unknown")).alias("promo_filled"),
    )
)

enriched_carts = valid.withColumn("qty_percent_rank", F.percent_rank().over(w5))

with_windows = (
    enriched_carts
    .select(
        "*",
        F.percent_rank().over(w_hist).alias("qty_max_30m_percent_rank"),
        F.stddev_samp("qty").over(w7).alias("promo_stddev_samp_qty"),
    )
)

result = (
    with_windows
    .groupBy("promo")
    .agg(
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("num_promos").desc())
)
