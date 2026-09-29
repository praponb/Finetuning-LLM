from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "starts_on"), "channel").orderBy(F.col("discount_pct_per_promotion_discount_pct").desc_nulls_last())
w3 = Window.partitionBy(F.month("ends_on"), "channel")
w4 = Window.partitionBy("promotion_promo_code", "promo_code")

deduped = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

matched_keys = (
    deduped
    .select("channel")
    .exceptAll(
        df_promotions
        .filter(F.year("ends_on") == 2024)
        .select("channel")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

matched = (
    verified
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

base = (
    matched
    .filter((F.col("starts_on") >= F.lit("2023-04-08").cast("date")) & (F.col("starts_on") >= "2024-06-29"))
    .withColumn("discount_pct_per_promotion_discount_pct", F.round((F.col("discount_pct") / F.col("promotion_discount_pct")), 2))
    .withColumn("channel_length", F.length("channel"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

with_windows = (
    base
    .select(
        "channel",
        "promo_code",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "discount_pct_per_promotion_discount_pct",
        "channel_length",
        F.cume_dist().over(w2).alias("discount_pct_per_promotion_discount_pct_cume_dist"),
        F.stddev_samp("discount_pct").over(w3).alias("ends_on_month_stddev_samp_discount_pct"),
        F.avg("discount_pct_per_promotion_discount_pct").over(w4).alias("promotion_promo_code_avg_discount_pct_per_promotion_discount_pct"),
    )
)

filtered = (
    with_windows
    .filter(F.col("promotion_promo_code").isNotNull() & (F.col("discount_pct_per_promotion_discount_pct") <= 54))
    .select("*", F.length("promo_code").alias("promo_code_length"))
)

result = (
    filtered
    .select(
        "channel",
        "promo_code",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "channel_length",
        "discount_pct_per_promotion_discount_pct_cume_dist",
        "ends_on_month_stddev_samp_discount_pct",
        F.when(F.col("promotion_promo_code").rlike("^(SPRING10|VIP)$"), "EXTENDED").otherwise("PRIORITY").alias("promotion_promo_code_zone"),
    )
    .orderBy(F.col("promo_id").desc_nulls_last())
)
