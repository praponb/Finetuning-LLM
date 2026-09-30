from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w_recent = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy(F.col("promotion_discount_pct").desc_nulls_first())
w4 = Window.partitionBy("promotion_promo_code")
w5 = Window.partitionBy("promotion_promo_code").orderBy("promotion_discount_pct", "id", "promo_id")

ranked = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .withColumn("last4_max_discount_pct", F.max("discount_pct").over(w_recent))
    .withColumn("last4_count_discount_pct", F.count("discount_pct").over(w_recent))
    .withColumn("last4_sum_discount_pct", F.sum("discount_pct").over(w_recent))
    .withColumn("last4_avg_discount_pct", F.avg("discount_pct").over(w_recent))
)

with_window = (
    enriched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched_promotions = (
    with_window
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "row_num",
        "last4_max_discount_pct",
        "last4_sum_discount_pct",
        "last4_avg_discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.cume_dist().over(w_running).alias("promotion_discount_pct_cume_dist"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w4)), 4).alias("promo_id_share"),
        F.ntile(10).over(w5).alias("promotion_discount_pct_bucket"),
    )
)

cleaned = (
    enriched_promotions
    .filter(F.col("promotion_promo_code").isNotNull() & F.col("promo_code").isin(["SPRING10", "VIP"]))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "row_num",
        "last4_max_discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct_bucket",
        (F.col("last4_sum_discount_pct") - F.col("promo_id")).alias("last4_sum_discount_pct_minus_promo_id"),
    )
)

grouped = (
    cleaned
    .groupBy(
        F.date_trunc("MONTH", "ends_on").alias("activity_month"),
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
        F.percentile_approx("last4_sum_discount_pct_minus_promo_id", 0.9).alias("last4_sum_discount_pct_minus_promo_id_p90"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes_2"),
        F.percentile_approx("last4_max_discount_pct", 0.9).alias("last4_max_discount_pct_p90"),
    )
    .filter(F.col("last4_max_discount_pct_p90") > 0)
)

result = (
    grouped
    .select(
        "activity_month",
        "year_start",
        "num_promotion_promo_codes",
        "last4_sum_discount_pct_minus_promo_id_p90",
        "num_promotion_promo_codes_2",
        "last4_max_discount_pct_p90",
    )
)
