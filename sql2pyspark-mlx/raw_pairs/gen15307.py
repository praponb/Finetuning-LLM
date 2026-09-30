from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w3 = Window.orderBy(F.col("plan_max_tier_value").desc_nulls_first())
w4 = Window.orderBy(F.col("plan_max_tier_value").desc_nulls_last())
w5 = Window.orderBy(F.col("plan_max_tier_value_dense_rank").desc_nulls_last())
w6 = Window.orderBy(F.col("plan_max_tier_value_percent_rank_plus_tier_value_cume_dist_3").desc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("tier_value"))

scored = (
    lines
    .select(
        "id",
        "plan",
        "tier_value",
        F.max("tier_value").over(w).alias("plan_max_tier_value"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "plan",
        "tier_value",
        "plan_max_tier_value",
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist_2"),
        F.percent_rank().over(w3).alias("plan_max_tier_value_percent_rank"),
        F.dense_rank().over(w4).alias("plan_max_tier_value_dense_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist_3"),
    )
)

valid = (
    enriched
    .filter(F.col("plan_max_tier_value") <= 44)
    .select(
        "plan_max_tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "plan_max_tier_value_percent_rank",
        "plan_max_tier_value_dense_rank",
        "tier_value_cume_dist_3",
        (F.col("plan_max_tier_value_percent_rank") + F.col("tier_value_cume_dist_3")).alias("plan_max_tier_value_percent_rank_plus_tier_value_cume_dist_3"),
        F.round((F.col("plan_max_tier_value_percent_rank") / F.col("plan_max_tier_value_dense_rank")), 2).alias("plan_max_tier_value_percent_rank_per_plan_max_tier_value_dense_rank"),
    )
)

metrics = (
    valid
    .select(
        "plan_max_tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "plan_max_tier_value_percent_rank",
        "plan_max_tier_value_dense_rank",
        F.percent_rank().over(w5).alias("plan_max_tier_value_dense_rank_percent_rank"),
        F.rank().over(w6).alias("plan_max_tier_value_percent_rank_plus_tier_value_cume_dist_3_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("plan_max_tier_value_percent_rank").isNull())
    .select("*", F.ceil("tier_value_cume_dist_2").alias("tier_value_cume_dist_2_whole"))
)

result = (
    cleaned
    .select(
        "plan_max_tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "plan_max_tier_value_dense_rank",
        "plan_max_tier_value_dense_rank_percent_rank",
        "plan_max_tier_value_percent_rank_plus_tier_value_cume_dist_3_rank",
    )
)
