from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") >= F.lit("2023-07-10").cast("date")))
    .select("id", "channel", F.length("promo_code").alias("promo_code_length"))
)

valid = (
    cleaned
    .filter(F.col("promo_code_length").isNull())
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

cleaned_promotions = (
    valid
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "*",
        F.when(F.col("promo_code_length") >= 70, "High").when(F.col("promo_code_length") >= 36, "Medium").otherwise("Low").alias("promo_code_length_level"),
    )
)

cleaned_promotions_2 = (
    cleaned_promotions
    .filter(F.col("channel").isNotNull() & (F.col("channel") == "store"))
    .select(
        "id",
        "channel",
        "promo_code_length",
        "channel_title",
        "resolved_channel",
        "promo_code_length_level",
        F.ceil("promo_code_length").alias("promo_code_length_whole"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

totals = (
    cleaned_promotions_2
    .groupBy(
        F.when(F.col("promo_code_length_whole") >= 63, "PREMIUM").otherwise("STANDARD").alias("promo_code_length_whole_tier"),
    )
    .agg(
        F.round(F.avg("promo_code_length_whole"), 2).alias("avg_promo_code_length_whole"),
        F.sum(F.when(F.col("channel") == "web", F.col("promo_code_length"))).alias("web_total_promo_code_length"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("promo_code_length_whole") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("promo_code_length_whole_per_partner"),
        F.percentile_approx("promo_code_length", 0.5).alias("promo_code_length_p50"),
    )
)

result = (
    totals
    .select(
        "promo_code_length_whole_tier",
        "web_total_promo_code_length",
        "distinct_ids",
        "promo_code_length_whole_per_partner",
        "promo_code_length_p50",
    )
    .orderBy(F.col("web_total_promo_code_length").desc())
)
