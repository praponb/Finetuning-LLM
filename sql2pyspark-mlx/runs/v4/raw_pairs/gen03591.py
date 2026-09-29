from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("channel")
w3 = Window.orderBy("discount_pct", "id")
w_part = Window.partitionBy("promo_code").orderBy("ends_on", "id")

valid = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-07-19"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.substring("promo_code", 1, 3).alias("promo_code_prefix"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.count("discount_pct").over(w).alias("promo_code_count_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.ntile(5).over(w3).alias("discount_pct_bucket"),
    )
)

scored = (
    with_windows
    .withColumn("discount_pct_bucket_share", F.round((F.col("discount_pct_bucket") / F.sum("discount_pct_bucket").over(w)), 4))
    .withColumn("discount_pct_bucket_2", F.ntile(4).over(w3))
    .withColumn("prev_discount_pct_bucket", F.lag("discount_pct_bucket", 1).over(w_part))
)

cleaned = (
    scored
    .filter(F.col("discount_pct") > 0.26)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_code_count_discount_pct",
        "discount_pct_share",
        "discount_pct_bucket",
        "discount_pct_bucket_share",
        "discount_pct_bucket_2",
        "prev_discount_pct_bucket",
        (F.col("discount_pct_share") * F.col("discount_pct_bucket_2")).alias("discount_pct_share_times_discount_pct_bucket_2"),
    )
)

kept_keys = (
    cleaned
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("channel") == "partner")
        .select("id", "channel")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("discount_pct_share") > 0.74)
    .select("*", F.month("starts_on").alias("starts_on_month"))
)

result = (
    base
    .groupBy("channel")
    .pivot("promo_code", ["VIP", "BUNDLE"])
    .agg(F.avg("discount_pct_bucket_share").alias("amount"))
)
