from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("promo_id").desc_nulls_first(), "id", "promo_id")
w2 = Window.partitionBy(F.year("ends_on")).orderBy(F.col("promotion_discount_pct").desc_nulls_last())
w3 = Window.partitionBy("promo_code", "channel")
w4 = Window.partitionBy(F.month("ends_on"), "promotion_promo_code")
w5 = Window.partitionBy("promotion_promo_code")
w6 = Window.partitionBy(F.year("starts_on"), "promo_code")
w7 = Window.partitionBy(F.month("starts_on"), "promotion_promo_code").orderBy(F.col("promo_id_share").desc())
w8 = Window.partitionBy(F.month("starts_on"), "channel")
w9 = Window.partitionBy("channel")
w10 = Window.partitionBy(F.date_trunc("YEAR", "ends_on"), "promo_code")
w11 = Window.partitionBy("channel").orderBy("discount_pct")
w12 = Window.orderBy(F.col("promo_code_count_promo_id_share").desc_nulls_last())
w13 = Window.partitionBy("promo_code")

matched = (
    df_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

remaining_keys = (
    deduped
    .select("promo_code")
    .intersect(
        df_promotions
        .filter(F.col("channel").isNull())
        .select("promo_code")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w2).alias("promotion_discount_pct_dense_rank"),
        F.count("promo_id").over(w3).alias("promo_code_count_promo_id"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w5)), 4).alias("promo_id_share"),
        F.count("discount_pct").over(w6).alias("starts_on_year_count_discount_pct"),
    )
)

with_windows = (
    metrics
    .filter(F.col("promo_id").isNull())
    .select(
        "*",
        F.rank().over(w7).alias("promo_id_share_rank"),
        F.round((F.col("promo_code_count_promo_id") / F.sum("promo_code_count_promo_id").over(w5)), 4).alias("promo_code_count_promo_id_share"),
        F.sum("promotion_discount_pct_dense_rank").over(w8).alias("starts_on_month_sum_promotion_discount_pct_dense_rank"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w9)), 4).alias("discount_pct_share_2"),
    )
)

with_windows_promotions = (
    with_windows
    .select(
        "id",
        "promo_code",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promotion_discount_pct_dense_rank",
        "promo_code_count_promo_id",
        "promo_id_share",
        "promo_code_count_promo_id_share",
        "starts_on_month_sum_promotion_discount_pct_dense_rank",
        "discount_pct_share_2",
        F.min("promotion_discount_pct").over(w10).alias("ends_on_period_min_promotion_discount_pct"),
        F.min("discount_pct").over(w9).alias("channel_min_discount_pct"),
        F.percent_rank().over(w11).alias("discount_pct_percent_rank"),
        F.avg("promo_id_share_rank").over(w3).alias("promo_code_avg_promo_id_share_rank"),
    )
)

with_windows_promotions_2 = (
    with_windows_promotions
    .select(
        "id",
        "promo_code_count_promo_id",
        "promo_id_share",
        "starts_on_month_sum_promotion_discount_pct_dense_rank",
        "discount_pct_share_2",
        "discount_pct_percent_rank",
        F.rank().over(w12).alias("promo_code_count_promo_id_share_rank"),
        F.max("discount_pct_percent_rank").over(w13).alias("promo_code_max_discount_pct_percent_rank"),
    )
)

result = (
    with_windows_promotions_2
    .cube(
        F.when(F.col("promo_id_share") >= 0.86, "PREMIUM").otherwise("STANDARD").alias("promo_id_share_tier"),
    )
    .agg(
        F.round(F.avg("discount_pct_share_2"), 2).alias("avg_discount_pct_share_2"),
        F.percentile_approx("starts_on_month_sum_promotion_discount_pct_dense_rank", [0.25, 0.5, 0.75]).alias("starts_on_month_sum_promotion_discount_pct_dense_rank_quartiles"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("promo_code_count_promo_id_share_rank", 0.95).alias("promo_code_count_promo_id_share_rank_p95"),
        F.percentile_approx("starts_on_month_sum_promotion_discount_pct_dense_rank", [0.25, 0.5, 0.75]).alias("starts_on_month_sum_promotion_discount_pct_dense_rank_quartiles_2"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy("row_count")
)
