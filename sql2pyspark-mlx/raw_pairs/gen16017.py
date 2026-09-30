from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-09-15").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.floor("discount_pct").alias("discount_pct_whole"))
)

matched = (
    filtered
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

remaining_keys = (
    with_window
    .select("id", "promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("channel").isin(["store", "mobile"]))
        .select("id", "promo_code")
    )
)

verified = (
    with_window
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "channel",
        "promo_code",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        "promotion_discount_pct_2",
    )
    .orderBy("id")
)
