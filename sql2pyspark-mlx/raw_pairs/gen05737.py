from pyspark.sql import functions as F

matched_keys = (
    df_promotions
    .select("promo_code", "id")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") > 0.12)
        .select("promo_code", "id")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("discount_pct") >= 0.35, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        F.date_trunc("WEEK", "ends_on").alias("week_start"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes_2"),
    )
)
