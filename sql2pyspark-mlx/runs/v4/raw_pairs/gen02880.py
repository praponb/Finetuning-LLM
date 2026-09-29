from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency", "plan")
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy("currency_stddev_samp_tier_value")
w5 = Window.partitionBy("plan").orderBy("tier_value")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

filtered = (
    lines
    .filter(F.col("plan") != "starter")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        F.trim("currency").alias("clean_currency"),
        F.lower("currency").alias("currency_lower"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.min("tier_value").over(w).alias("plan_min_tier_value"),
        F.stddev_samp("tier_value").over(w2).alias("currency_stddev_samp_tier_value"),
    )
)

scored = (
    enriched
    .withColumn("currency_min_currency_stddev_samp_tier_value", F.min("currency_stddev_samp_tier_value").over(w3))
    .withColumn("currency_stddev_samp_tier_value_cume_dist", F.cume_dist().over(w4))
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w5).alias("tier_value_dense_rank"),
        F.sum("currency_min_currency_stddev_samp_tier_value").over(w2).alias("currency_sum_currency_min_currency_stddev_samp_tier_value"),
    )
)

scored_price_tiers = (
    with_windows
    .select(
        "*",
        F.avg("tier_value_dense_rank").over(w3).alias("currency_avg_tier_value_dense_rank"),
        F.min("tier_value_dense_rank").over(w2).alias("currency_min_tier_value_dense_rank"),
        F.max("currency_sum_currency_min_currency_stddev_samp_tier_value").over(w2).alias("currency_max_currency_sum_currency_min_currency_stddev_samp_tier_value"),
    )
)

result = (
    scored_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        "clean_currency",
        "currency_lower",
        "plan_min_tier_value",
        "currency_stddev_samp_tier_value",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "GROUP_B").when(F.col("currency").rlike("^(EUR)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("currency_zone"),
    )
)
