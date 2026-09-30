from pyspark.sql import functions as F

attributed = (
    df_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .cube(
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
        F.date_trunc("QUARTER", "ends_on").alias("quarter_start"),
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("discount_pct_per_spring10"),
        F.round(F.sum("promotion_discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("promotion_discount_pct_per_bundle"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
    )
    .orderBy("store_events")
)
