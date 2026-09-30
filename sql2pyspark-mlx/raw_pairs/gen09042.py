from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("ends_on_period"), "promo_code")
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w4 = Window.partitionBy("channel").orderBy("ends_on", "id")
w5 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("channel").orderBy("prev_discount_pct", "id")
w7 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy(F.col("discount_pct_share").desc_nulls_last())
w9 = Window.partitionBy("channel").orderBy("discount_pct_avg_2_prior", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("channel").orderBy(F.col("prev_discount_pct").desc())
w11 = Window.partitionBy("channel").orderBy("discount_pct_max_7d", "id")

cleaned = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-05-26"))) & (F.col("discount_pct") >= 0.29))
    .select("*", F.date_trunc("month", "ends_on").alias("ends_on_period"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on_period",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.max("discount_pct").over(w2).alias("discount_pct_max_7d"),
        F.ntile(4).over(w3).alias("discount_pct_bucket"),
        F.lag("discount_pct", 1).over(w4).alias("prev_discount_pct"),
        F.avg("discount_pct").over(w5).alias("discount_pct_avg_2_prior"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct_share",
        "discount_pct_max_7d",
        "discount_pct_bucket",
        "discount_pct_avg_2_prior",
        F.last("discount_pct_avg_2_prior", ignorenulls=True).over(w7).alias("last_known_discount_pct_avg_2_prior"),
        F.dense_rank().over(w8).alias("discount_pct_share_dense_rank"),
        F.avg("discount_pct_avg_2_prior").over(w9).alias("discount_pct_avg_2_prior_avg_3"),
        F.percent_rank().over(w10).alias("prev_discount_pct_percent_rank"),
        F.lead("discount_pct_max_7d", 1).over(w11).alias("next_discount_pct_max_7d"),
    )
)

result = (
    scored
    .withColumn("activity_month", F.date_trunc("MONTH", "starts_on"))
    .withColumn("prev_discount_pct_percent_rank_tier", F.when(F.col("prev_discount_pct_percent_rank") >= 0.84, "PREMIUM").otherwise("STANDARD"))
    .rollup("activity_month", "prev_discount_pct_percent_rank_tier")
    .agg(
        F.sum("discount_pct_max_7d").alias("total_discount_pct_max_7d"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_discount_pct_percent_rank", [0.25, 0.5, 0.75]).alias("prev_discount_pct_percent_rank_quartiles"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .orderBy(F.col("activity_month").desc())
)
