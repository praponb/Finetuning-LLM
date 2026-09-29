from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-08-05"))) & F.col("discount_pct").between(0.2, 0.25))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

filtered_promotions = (
    filtered
    .filter(F.col("ends_on") > F.lit("2023-10-02").cast("date"))
    .withColumn("starts_on_plus_20d", F.date_add("starts_on", 20))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

kept_keys = (
    filtered_promotions
    .select("promo_code")
    .intersect(
        df_promotions
        .filter(F.month("ends_on").isin([8, 9, 10]))
        .select("promo_code")
    )
)

kept_rows = (
    filtered_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "channel",
        "promo_code",
        "starts_on",
        "days_since_ends_on",
        "resolved_channel",
        "starts_on_plus_20d",
        "channel_clean",
        F.when(F.col("promo_code").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("days_since_ends_on").desc_nulls_first())
)
