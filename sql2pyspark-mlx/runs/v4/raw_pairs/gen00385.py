from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("bundle_events").desc())

attributed = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    attributed
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched = (
    with_window
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

with_window_promotions = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_4"),
        F.col("pr.promo_code").alias("promotion_promo_code_4"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_4"),
    )
)

valid = (
    with_window_promotions
    .filter((F.col("ends_on") > F.lit("2024-05-15").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        (F.col("promo_id_2") * F.col("promotion_discount_pct")).alias("promo_id_2_times_promotion_discount_pct"),
    )
)

result = (
    valid
    .groupBy(
        F.date_trunc("month", "starts_on").alias("activity_month"),
        F.date_trunc("quarter", "starts_on").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promotion_promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.percentile_approx("promotion_discount_pct", [0.25, 0.5, 0.75]).alias("promotion_discount_pct_quartiles"),
        F.count_if(F.col("promotion_promo_code_3") == "BUNDLE").alias("bundle_events"),
    )
    .withColumn("bundle_events_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("bundle_events").desc())
)
