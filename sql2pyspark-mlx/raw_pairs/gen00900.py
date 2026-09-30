from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code_2")

matched_keys = (
    df_promotions
    .select("promo_code", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on") >= "2024-09-01")
        .select("promo_code", "channel")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_window = (
    reconciled
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window_promotions = (
    with_window
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

metrics = (
    with_window_promotions
    .filter(F.col("discount_pct") > 0.33)
    .withColumn("promo_id_2_share", F.round((F.col("promo_id_2") / F.sum("promo_id_2").over(w)), 4))
)

attributed = (
    metrics
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    attributed
    .groupBy("channel")
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.sum(F.when(F.col("promotion_promo_code_3") == "WELCOME5", F.col("promo_id_3")).otherwise(0)).alias("welcome5_promo_id_3"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct_2"),
        F.avg(F.when(F.col("promotion_promo_code") == "BUNDLE", F.col("discount_pct"))).alias("bundle_avg_discount_pct"),
        F.avg(F.when(F.col("channel") == "web", F.col("promo_id_2"))).alias("web_avg_promo_id_2"),
        F.percentile_approx("promotion_discount_pct_2", 0.9).alias("promotion_discount_pct_2_p90"),
    )
    .filter(F.col("avg_discount_pct") < 1000000)
    .orderBy(F.col("avg_discount_pct").desc_nulls_first())
)
