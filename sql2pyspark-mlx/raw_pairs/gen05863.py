from pyspark.sql import functions as F

attributed = (
    df_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed_promotions = (
    attributed
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

attributed_promotions_2 = (
    attributed_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

matched = (
    attributed_promotions_2
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_4"),
        F.col("pr.promo_code").alias("promotion_promo_code_4"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_4"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("promotion_discount_pct") >= 0.27, "PREMIUM").otherwise("STANDARD").alias("promotion_discount_pct_tier"),
        "promotion_promo_code_4",
        "promo_id",
    )
    .agg(
        F.sum(F.when(F.col("promotion_promo_code_3") == "WELCOME5", F.col("promotion_discount_pct_3")).otherwise(0)).alias("welcome5_promotion_discount_pct_3"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.countDistinct("promotion_promo_code_4").alias("distinct_promotion_promo_code_4s"),
        F.sort_array(F.collect_set("promotion_promo_code_3")).alias("promotion_promo_code_3_list"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("welcome5_promotion_discount_pct_3") >= 0)
)
