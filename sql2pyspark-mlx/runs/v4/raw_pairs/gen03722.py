from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-05-21").cast("date")) & F.col("channel").isNotNull() & (F.col("ends_on") <= "2025-04-09"))
    .withColumn("promo_code_upper", F.upper("promo_code"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

matched_keys = (
    valid
    .select("promo_code")
    .exceptAll(
        df_promotions
        .filter(F.col("promo_code") == "BUNDLE")
        .select("promo_code")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("channel", "promo_code")
    .intersect(
        df_promotions
        .filter(F.col("discount_pct") > 0.24)
        .select("channel", "promo_code")
    )
)

reconciled_promotions = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled_promotions
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.month("starts_on").isin([5, 7, 9]))
        .select("id")
    )
)

verified = (
    reconciled_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "starts_on", "discount_pct", "promo_code_upper", "resolved_channel")
    .orderBy(F.col("starts_on").asc_nulls_last())
)
