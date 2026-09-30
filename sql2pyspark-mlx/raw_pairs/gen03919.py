from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("channel", "promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("promo_code") == "WELCOME5")
        .select("channel", "promo_code")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("quarter", "ends_on").alias("quarter_start"),
        "channel",
        "promo_code",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount_pct")).otherwise(0)).alias("mobile_discount_pct"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
)
