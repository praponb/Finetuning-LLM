from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy(F.month("effective_from"), "plan")
w5 = Window.partitionBy(F.year("effective_from"), "plan")

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

enriched = (
    lines
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("plan_avg_tier_value", F.avg("tier_value").over(w2))
    .withColumn("tier_value_rank_2", F.rank().over(w3))
    .withColumn("effective_from_month_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w4))
)

scored = (
    enriched
    .filter(F.col("effective_from") >= "2025-04-14")
    .select(
        "id",
        "plan",
        "tier_value",
        "tier_value_rank_2",
        F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w5)), 4).alias("tier_value_rank_share"),
    )
)

filtered = (
    scored
    .filter(F.col("plan").isin(["pro", "enterprise", "basic"]))
    .select(
        "plan",
        "tier_value",
        "tier_value_rank_2",
        F.when(F.col("plan").isin(["enterprise", "starter"]), "Group A").otherwise("Other").alias("plan_group"),
    )
)

result = (
    filtered
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 63, "LARGE").otherwise("SMALL"))
    .cube("plan", "tier_value_tier")
    .agg(
        F.percentile_approx("tier_value_rank_2", 0.9).alias("tier_value_rank_2_p90"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.grouping("plan").alias("gid"),
    )
    .filter(F.col("tier_value_rank_2_p90") < 1000000)
)
