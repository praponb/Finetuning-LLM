from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w_recent = Window.partitionBy(F.date_trunc("week", "effective_from"), "currency")
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w_part = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("currency", "plan").orderBy(F.col("effective_from_period_avg_tier_value").desc())
w7 = Window.partitionBy(F.date_trunc("MONTH", "effective_from"), "currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

metrics = (
    exploded
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("effective_from_period_avg_tier_value", F.avg("tier_value").over(w_recent))
    .withColumn("tier_value_percent_rank_2", F.percent_rank().over(w3))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w_part))
)

filtered = (
    metrics
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-02-09"))) & (F.col("tier_value_dense_rank") >= 6))
    .select("*", F.length("plan").alias("plan_length"))
)

metrics_price_tiers = (
    filtered
    .select(
        "tier_value",
        "tier_value_percent_rank",
        "tier_value_percent_rank_2",
        "tier_value_dense_rank",
        F.round((F.col("tier_value_dense_rank") / F.sum("tier_value_dense_rank").over(w5)), 4).alias("tier_value_dense_rank_share"),
        F.cume_dist().over(w6).alias("effective_from_period_avg_tier_value_cume_dist"),
        F.min("plan_length").over(w7).alias("effective_from_period_min_plan_length"),
    )
)

report = (
    metrics_price_tiers
    .select(
        "tier_value_percent_rank",
        "tier_value_percent_rank_2",
        "tier_value_dense_rank",
        "tier_value_dense_rank_share",
        "effective_from_period_min_plan_length",
    )
    .orderBy(
        "effective_from_period_min_plan_length",
        F.col("tier_value_percent_rank_2").desc(),
    )
)

result = (
    report
    .select(
        "tier_value_percent_rank",
        "tier_value_percent_rank_2",
        "tier_value_dense_rank",
        "tier_value_dense_rank_share",
        "effective_from_period_min_plan_length",
    )
    .orderBy(
        F.col("tier_value_dense_rank").desc_nulls_last(),
        "tier_value_percent_rank",
    )
)
