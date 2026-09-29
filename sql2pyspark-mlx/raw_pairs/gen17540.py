from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("ends_on"), "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("starts_on", "id")
w3 = Window.partitionBy(F.year("ends_on"), "channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("running_discount_pct").asc_nulls_last())
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("channel").orderBy("prev_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("cumulative_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("prev_discount_pct").desc(), "id")
w9 = Window.orderBy(F.col("cumulative_discount_pct_share").desc_nulls_first())
w_hist = Window.orderBy("cumulative_discount_pct_times_prev_discount_pct_bucket", "id").rowsBetween(-6, -1)
w_hist_2 = Window.orderBy("prev_discount_pct_bucket", "id")

with_windows = (
    df_promotions
    .filter(~F.col("promo_code").isin(["VIP", "SPRING10"]))
    .select(
        "id",
        "channel",
        "promo_code",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.lag("discount_pct", 1).over(w2).alias("prev_discount_pct"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "cumulative_discount_pct",
        "prev_discount_pct",
        F.dense_rank().over(w4).alias("running_discount_pct_dense_rank"),
        F.round((F.col("cumulative_discount_pct") / F.sum("cumulative_discount_pct").over(w5)), 4).alias("cumulative_discount_pct_share"),
        F.round((F.col("prev_discount_pct") / F.sum("prev_discount_pct").over(w5)), 4).alias("prev_discount_pct_share"),
        F.last("cumulative_discount_pct", ignorenulls=True).over(w6).alias("last_known_cumulative_discount_pct"),
        F.last("channel", ignorenulls=True).over(w7).alias("last_known_channel"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "cumulative_discount_pct",
        F.ntile(5).over(w8).alias("prev_discount_pct_bucket"),
        F.percent_rank().over(w9).alias("cumulative_discount_pct_share_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("prev_discount_pct_bucket").isNull())
    .select(
        "*",
        (F.col("cumulative_discount_pct") * F.col("prev_discount_pct_bucket")).alias("cumulative_discount_pct_times_prev_discount_pct_bucket"),
        (F.col("cumulative_discount_pct") - F.col("prev_discount_pct_bucket")).alias("cumulative_discount_pct_minus_prev_discount_pct_bucket"),
    )
)

cleaned = (
    base
    .filter(F.col("prev_discount_pct_bucket") > 4)
    .withColumn("cumulative_discount_pct_times_prev_discount_pct_bucket_level", F.when(F.col("cumulative_discount_pct_times_prev_discount_pct_bucket") >= 86, "High").when(F.col("cumulative_discount_pct_times_prev_discount_pct_bucket") >= 26, "Medium").otherwise("Low"))
)

filtered = (
    cleaned
    .filter(F.col("cumulative_discount_pct_minus_prev_discount_pct_bucket") > 78)
    .select(
        "*",
        F.round("cumulative_discount_pct", 0).alias("cumulative_discount_pct_rounded"),
    )
)

with_windows_promotions = (
    filtered
    .select(
        "id",
        "cumulative_discount_pct",
        "cumulative_discount_pct_share_percent_rank",
        "cumulative_discount_pct_times_prev_discount_pct_bucket",
        "cumulative_discount_pct_minus_prev_discount_pct_bucket",
        F.avg("cumulative_discount_pct").over(w_hist).alias("cumulative_discount_pct_avg_6_prior"),
        F.lead("prev_discount_pct_bucket", 2, 0).over(w_hist_2).alias("next_prev_discount_pct_bucket"),
    )
)

result = (
    with_windows_promotions
    .cube(
        F.when(F.col("cumulative_discount_pct_times_prev_discount_pct_bucket") >= 90, "LARGE").otherwise("SMALL").alias("cumulative_discount_pct_times_prev_discount_pct_bucket_tier"),
        F.when(F.col("cumulative_discount_pct_share_percent_rank") >= 0.82, "HIGH").otherwise("LOW").alias("cumulative_discount_pct_share_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("next_prev_discount_pct_bucket"), 2).alias("avg_next_prev_discount_pct_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("cumulative_discount_pct_avg_6_prior", 0.9).alias("cumulative_discount_pct_avg_6_prior_p90"),
        F.percentile_approx("cumulative_discount_pct", 0.95).alias("cumulative_discount_pct_p95"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc())
)
