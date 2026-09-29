from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("tier_value")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w_part = Window.partitionBy("plan").orderBy(F.col("tier_value_percent_rank_2").desc())
w_running = Window.orderBy(F.col("tier_value_percent_rank_2").desc_nulls_first())

lines = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("tier_value"))

scored = lines.select("*", F.percent_rank().over(w).alias("tier_value_percent_rank"))

enriched = (
    scored
    .withColumn("plan_stddev_samp_tier_value_percent_rank", F.stddev_samp("tier_value_percent_rank").over(w2))
    .withColumn("tier_value_percent_rank_2", F.percent_rank().over(w3))
)

cleaned = (
    enriched
    .filter(F.col("plan").isin(["basic", "starter", "pro"]))
    .select(
        "*",
        F.ceil("tier_value_percent_rank").alias("tier_value_percent_rank_whole"),
    )
)

scored_price_tiers = (
    cleaned
    .select(
        "id",
        "plan",
        F.count("tier_value").over(w2).alias("plan_count_tier_value"),
        F.dense_rank().over(w_part).alias("tier_value_percent_rank_2_dense_rank"),
        F.rank().over(w_running).alias("tier_value_percent_rank_2_rank"),
    )
)

result = (
    scored_price_tiers
    .select("id", "plan_count_tier_value", "tier_value_percent_rank_2_rank")
    .orderBy("id")
)
