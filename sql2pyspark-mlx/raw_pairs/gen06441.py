from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("effective_from")).orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy(F.year("effective_from"), "plan")
w3 = Window.orderBy("tier_value")
w4 = Window.partitionBy("currency").orderBy(F.col("effective_from_year_stddev_samp_tier_value").desc())
w_part = Window.partitionBy("plan")
w_grp = Window.partitionBy("plan").orderBy(F.col("effective_from_year_stddev_samp_tier_value_percent_rank").desc_nulls_last())
w7 = Window.partitionBy("plan").orderBy(F.col("effective_from_year_stddev_samp_tier_value").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

enriched = (
    flattened
    .filter(F.col("effective_from") < "2024-09-17")
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.stddev_samp("tier_value").over(w2).alias("effective_from_year_stddev_samp_tier_value"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank_2"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "plan",
        "tier_value_percent_rank",
        "effective_from_year_stddev_samp_tier_value",
        F.percent_rank().over(w4).alias("effective_from_year_stddev_samp_tier_value_percent_rank"),
    )
)

scored = (
    enriched_price_tiers
    .select(
        "plan",
        "tier_value_percent_rank",
        F.avg("effective_from_year_stddev_samp_tier_value").over(w_part).alias("plan_avg_effective_from_year_stddev_samp_tier_value"),
        F.rank().over(w_grp).alias("effective_from_year_stddev_samp_tier_value_percent_rank_rank"),
        F.rank().over(w7).alias("effective_from_year_stddev_samp_tier_value_rank"),
        F.max("effective_from_year_stddev_samp_tier_value_percent_rank").over(w_part).alias("plan_max_effective_from_year_stddev_samp_tier_value_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("plan_avg_effective_from_year_stddev_samp_tier_value") >= 72, "LARGE").otherwise("SMALL").alias("plan_avg_effective_from_year_stddev_samp_tier_value_tier"),
        "plan",
    )
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("plan_max_effective_from_year_stddev_samp_tier_value_percent_rank")).otherwise(0)).alias("basic_plan_max_effective_from_year_stddev_samp_tier_value_percent_rank"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("tier_value_percent_rank", 0.5).alias("tier_value_percent_rank_p50"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("tier_value_percent_rank_p50").asc_nulls_last())
)
