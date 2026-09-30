from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy(F.year("effective_from"), "plan").orderBy(F.col("tier_value").desc_nulls_first())
w_running = Window.orderBy(F.col("plan_avg_tier_value").desc_nulls_first())
w6 = Window.partitionBy("plan").orderBy("tier_value")
w7 = Window.partitionBy("plan").orderBy(F.col("plan_avg_tier_value_rank").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("tier_value_percent_rank_3").desc())
w9 = Window.partitionBy("plan").orderBy("tier_value_rank")
w10 = Window.partitionBy("plan").orderBy("plan_avg_tier_value_rank_cume_dist")
w_grp = Window.orderBy(F.col("tier_value_rank_rank").desc_nulls_last())
w12 = Window.orderBy(F.col("tier_value_percent_rank").desc_nulls_last())
w13 = Window.orderBy(F.col("tier_value_percent_rank").desc_nulls_first())
w_hist = Window.orderBy(F.col("plan_count_tier_value_percent_rank").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.avg("tier_value").over(w3).alias("plan_avg_tier_value"),
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.percent_rank().over(w4).alias("tier_value_percent_rank_2"),
    )
)

metrics_price_tiers = (
    metrics
    .select(
        "plan",
        "tier_value",
        "tier_value_rank",
        "tier_value_dense_rank",
        "plan_avg_tier_value",
        "tier_value_percent_rank",
        F.rank().over(w_running).alias("plan_avg_tier_value_rank"),
        F.percent_rank().over(w6).alias("tier_value_percent_rank_3"),
    )
)

cleaned = (
    metrics_price_tiers
    .filter(F.col("tier_value_rank") > 5)
    .select(
        "plan",
        "tier_value_rank",
        "plan_avg_tier_value",
        "tier_value_percent_rank",
        "plan_avg_tier_value_rank",
        "tier_value_percent_rank_3",
        (F.col("tier_value_rank") - F.col("tier_value")).alias("tier_value_rank_minus_tier_value"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.stddev_samp("tier_value_percent_rank").over(w3).alias("plan_stddev_samp_tier_value_percent_rank"),
        F.cume_dist().over(w7).alias("plan_avg_tier_value_rank_cume_dist"),
        F.cume_dist().over(w8).alias("tier_value_percent_rank_3_cume_dist"),
        F.rank().over(w9).alias("tier_value_rank_rank"),
    )
)

with_windows = (
    scored
    .select(
        "plan_avg_tier_value",
        "tier_value_percent_rank",
        "plan_avg_tier_value_rank",
        "tier_value_rank_minus_tier_value",
        "plan_avg_tier_value_rank_cume_dist",
        "tier_value_rank_rank",
        F.count("tier_value_percent_rank").over(w3).alias("plan_count_tier_value_percent_rank"),
        F.cume_dist().over(w10).alias("plan_avg_tier_value_rank_cume_dist_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "plan_avg_tier_value_rank",
        "tier_value_rank_rank",
        "plan_avg_tier_value_rank_cume_dist_cume_dist",
        F.percent_rank().over(w_grp).alias("tier_value_rank_rank_percent_rank"),
        F.percent_rank().over(w12).alias("tier_value_percent_rank_percent_rank"),
        F.rank().over(w13).alias("tier_value_percent_rank_rank"),
        F.percent_rank().over(w_hist).alias("plan_count_tier_value_percent_rank_percent_rank"),
    )
)
