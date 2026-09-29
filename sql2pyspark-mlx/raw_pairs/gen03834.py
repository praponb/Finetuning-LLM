from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("discount_pct", "id")
w_running_2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .withColumn("discount_pct_level", F.when(F.col("discount_pct") >= 0.26, "High").when(F.col("discount_pct") >= 0.14, "Medium").otherwise("Low"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("NONE")))
)

enriched = (
    valid
    .select(
        "*",
        F.ntile(4).over(w_running).alias("discount_pct_bucket"),
        F.avg("discount_pct").over(w_running_2).alias("discount_pct_avg_5"),
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel"),
    )
)

attributed = (
    enriched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed_promotions = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("s.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched = (
    attributed_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

totals = (
    matched
    .withColumn("promotion_discount_pct_3_tier", F.when(F.col("promotion_discount_pct_3") >= 0.26, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["promotion_discount_pct_3_tier", "promotion_promo_code_2", "promotion_promo_code_3"], ["promotion_discount_pct_3_tier", "promotion_promo_code_2"]],
        "promotion_discount_pct_3_tier",
        "promotion_promo_code_2",
        "promotion_promo_code_3",
    )
    .agg(
        F.round(F.sum("promo_id_3") / F.nullif(F.count(F.when(F.col("promotion_promo_code_3") == "SPRING10", 1)), F.lit(0)), 2).alias("promo_id_3_per_spring10"),
        F.percentile_approx("promo_id_2", 0.9).alias("promo_id_2_p90"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.sum(F.when(F.col("promotion_promo_code") == "WELCOME5", F.col("promotion_discount_pct_3"))).alias("welcome5_total_promotion_discount_pct_3"),
    )
)

result = (
    totals
    .select(
        "promotion_discount_pct_3_tier",
        "promotion_promo_code_2",
        "promotion_promo_code_3",
        "promo_id_3_per_spring10",
        "promo_id_2_p90",
    )
)
