from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "starts_on"), "channel").orderBy("starts_on", "id")
w2 = Window.partitionBy(F.month("ends_on"), "channel")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w5 = Window.partitionBy("channel", "promo_code").orderBy(F.col("ends_on").desc(), "id")
w6 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code")
w8 = Window.partitionBy(F.month("ends_on"), "channel").orderBy(F.col("prev_discount_pct").asc_nulls_first())
w9 = Window.partitionBy("promo_code").orderBy(F.col("running_sum_row_seq").desc_nulls_last())
w10 = Window.partitionBy("channel").orderBy(F.col("promo_id").asc_nulls_first(), "id", "promo_id")

enriched = (
    df_promotions
    .filter(~F.col("promo_code").isin(["WELCOME5", "SPRING10", "VIP"]))
    .select(
        "*",
        F.lag("discount_pct", 2).over(w).alias("prev_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_6_prior"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

scored = (
    enriched
    .withColumn("running_max_row_seq", F.max("row_seq").over(w6))
    .withColumn("running_sum_row_seq", F.sum("row_seq").over(w6))
    .withColumn("running_avg_row_seq", F.avg("row_seq").over(w6))
)

scored_promotions = (
    scored
    .withColumn("prev_discount_pct_share", F.round((F.col("prev_discount_pct") / F.sum("prev_discount_pct").over(w7)), 4))
    .withColumn("prev_discount_pct_rank", F.rank().over(w8))
)

latest = (
    scored_promotions
    .withColumn("rank_in_group", F.rank().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

attributed = (
    latest
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    attributed
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_avg_6_prior",
        "event_seq",
        "running_sum_row_seq",
        "running_avg_row_seq",
        "prev_discount_pct_share",
        "prev_discount_pct_rank",
        "rank_in_group",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
    )
)

cleaned = (
    ranked
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-04-05"))) & (F.col("starts_on") > "2024-03-01"))
    .select(
        "id",
        "discount_pct",
        "discount_pct_avg_6_prior",
        "running_sum_row_seq",
        "running_avg_row_seq",
        "prev_discount_pct_share",
        "rank_in_group",
        "promo_id",
        "promotion_promo_code",
        F.ceil("promotion_discount_pct").alias("promotion_discount_pct_whole"),
    )
)

result = (
    cleaned
    .groupBy(
        "promo_id",
        F.when(F.col("promotion_discount_pct_whole") >= 74, "PREMIUM").otherwise("STANDARD").alias("promotion_discount_pct_whole_tier"),
    )
    .agg(
        F.percentile_approx("promo_id", 0.95).alias("promo_id_p95"),
        F.countDistinct("promotion_promo_code").alias("distinct_promotion_promo_codes"),
        F.percentile_approx("discount_pct_avg_6_prior", 0.95).alias("discount_pct_avg_6_prior_p95"),
    )
)
