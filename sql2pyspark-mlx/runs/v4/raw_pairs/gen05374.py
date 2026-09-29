from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy("promotion_promo_code", "promo_code")
w3 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(-6, -1)
w4 = Window.partitionBy("promotion_promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(-5, -1)
w_recent = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("promo_id_avg_6_prior")
w6 = Window.partitionBy("promo_code", "promotion_promo_code").orderBy(F.col("discount_pct").desc(), "id", "promo_id")

deduped = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_window = (
    deduped
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    with_window
    .select(
        "*",
        F.round((F.col("promo_id") / F.sum("promo_id").over(w2)), 4).alias("promo_id_share"),
        F.avg("promo_id").over(w3).alias("promo_id_avg_6_prior"),
    )
)

cleaned = (
    scored
    .filter((F.col("ends_on") > F.lit("2023-12-05").cast("date")) & ~F.col("promo_code").isin(["BUNDLE", "WELCOME5"]))
    .select(
        "*",
        F.round((F.col("promo_id") * 1.1), 2).alias("promo_id_adjusted"),
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
        F.coalesce("promotion_promo_code", F.lit("OTHER")).alias("promotion_promo_code_clean"),
    )
)

with_windows = (
    cleaned
    .withColumn("promo_id_avg_6_prior_avg_5_prior", F.avg("promo_id_avg_6_prior").over(w4))
    .withColumn("promo_id_avg_6_prior_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("discount_pct_bucket", F.ntile(5).over(w6))
)

with_window_promotions = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window_promotions
    .withColumn("quarter_start", F.date_trunc("QUARTER", "starts_on"))
    .groupingSets(
        [["promo_id", "quarter_start"], ["promo_id"], []],
        "promo_id",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("promo_id_adjusted", 0.95).alias("promo_id_adjusted_p95"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.round(F.avg("promo_id_avg_6_prior_avg_5_prior"), 2).alias("avg_promo_id_avg_6_prior_avg_5_prior"),
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
    )
    .orderBy("quarter_start")
)
