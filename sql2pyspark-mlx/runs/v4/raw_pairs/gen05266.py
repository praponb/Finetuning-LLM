from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-05-09")))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.add_months("ends_on", 1).alias("ends_on_plus_months"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

attributed = (
    base
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

valid = (
    matched
    .filter(F.col("promo_id_2") > 9)
    .withColumn("channel_prefix", F.substring("channel", 1, 2))
    .withColumn("least_discount_pct_promo_id_2", F.least("discount_pct", "promo_id_2"))
    .withColumn("promotion_promo_code_filled", F.coalesce("promotion_promo_code", F.lit("NONE")))
)

summary = (
    valid
    .cube("channel", "promo_code")
    .agg(
        F.countDistinct("promotion_promo_code").alias("distinct_promotion_promo_codes"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("least_discount_pct_promo_id_2"), 2).alias("avg_least_discount_pct_promo_id_2"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    summary
    .select("channel", "num_rows")
    .orderBy("channel")
)
