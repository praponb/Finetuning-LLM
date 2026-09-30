from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("adjusted_tier_value").desc())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("plan_stddev_samp_adjusted_tier_value_dense_rank").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

filtered = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-04-08").cast("date"))
    .select(
        "plan",
        "currency",
        "tier_value",
        F.round((F.col("tier_value") * 0.85), 2).alias("adjusted_tier_value"),
    )
)

scored = (
    filtered
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("adjusted_tier_value_dense_rank", F.dense_rank().over(w2))
)

base = (
    scored
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .select(
        "plan",
        "tier_value",
        "tier_value_rank",
        "adjusted_tier_value_dense_rank",
        F.ceil("tier_value").alias("tier_value_whole"),
    )
)

scored_price_tiers = (
    base
    .withColumn("plan_stddev_samp_adjusted_tier_value_dense_rank", F.stddev_samp("adjusted_tier_value_dense_rank").over(w3))
    .withColumn("adjusted_tier_value_dense_rank_share", F.round((F.col("adjusted_tier_value_dense_rank") / F.sum("adjusted_tier_value_dense_rank").over(w3)), 4))
    .withColumn("plan_max_adjusted_tier_value_dense_rank", F.max("adjusted_tier_value_dense_rank").over(w3))
    .withColumn("adjusted_tier_value_dense_rank_share_2", F.round((F.col("adjusted_tier_value_dense_rank") / F.sum("adjusted_tier_value_dense_rank").over(w3)), 4))
)

metrics = (
    scored_price_tiers
    .withColumn("plan_stddev_samp_adjusted_tier_value_dense_rank_percent_rank", F.percent_rank().over(w4))
)

result = (
    metrics
    .withColumn("adjusted_tier_value_dense_rank_share_tier", F.when(F.col("adjusted_tier_value_dense_rank_share") >= 0.74, "LARGE").otherwise("SMALL"))
    .rollup("adjusted_tier_value_dense_rank_share_tier", "plan")
    .agg(
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.sum(F.when(F.col("plan") == "starter", F.col("tier_value_rank"))).alias("starter_total_tier_value_rank"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("pro_count").desc())
)
