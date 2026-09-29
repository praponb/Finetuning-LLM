from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("tier_value")
w_grp_2 = Window.partitionBy("plan").orderBy(F.col("least_plan_sum_tier_value_plan_sum_tier_value_whole").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_sum_tier_value").desc_nulls_first())
w_ordered = Window.partitionBy("plan").orderBy("plan_sum_tier_value_whole")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.sum("tier_value").over(w).alias("plan_sum_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("effective_from") >= F.lit("2024-02-01").cast("date"))
    .withColumn("plan_sum_tier_value_whole", F.ceil("plan_sum_tier_value"))
    .withColumn("tier_value_dense_rank_band", F.when(F.col("tier_value_dense_rank") >= 9, "High").when(F.col("tier_value_dense_rank") >= 3, "Medium").otherwise("Low"))
)

base = (
    filtered
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-05-05")))
    .select(
        "*",
        F.least("plan_sum_tier_value", "plan_sum_tier_value_whole").alias("least_plan_sum_tier_value_plan_sum_tier_value_whole"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "effective_from",
        "plan_sum_tier_value_whole",
        F.min("plan_sum_tier_value_whole").over(w).alias("plan_min_plan_sum_tier_value_whole"),
        F.stddev_samp("plan_sum_tier_value").over(w).alias("plan_stddev_samp_plan_sum_tier_value"),
        F.rank().over(w_grp_2).alias("least_plan_sum_tier_value_plan_sum_tier_value_whole_rank"),
        F.cume_dist().over(w4).alias("plan_sum_tier_value_cume_dist"),
        F.rank().over(w_ordered).alias("plan_sum_tier_value_whole_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("effective_from") > F.lit("2024-03-01").cast("date"))
    .withColumn("plan_stddev_samp_plan_sum_tier_value_minus_plan_sum_tier_value_cume_dist", (F.col("plan_stddev_samp_plan_sum_tier_value") - F.col("plan_sum_tier_value_cume_dist")))
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
)

result = (
    valid
    .select(
        "id",
        "effective_from",
        "plan_sum_tier_value_whole",
        "least_plan_sum_tier_value_plan_sum_tier_value_whole_rank",
        "plan_sum_tier_value_whole_rank",
        "plan_stddev_samp_plan_sum_tier_value_minus_plan_sum_tier_value_cume_dist",
        "days_since_effective_from",
    )
    .orderBy("days_since_effective_from")
)
