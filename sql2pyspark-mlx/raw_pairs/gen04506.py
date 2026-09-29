from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("adjusted_tier_value").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy("tier_value")
w5 = Window.partitionBy("currency").orderBy(F.col("adjusted_tier_value").desc())
w6 = Window.partitionBy("currency").orderBy(F.col("tier_value_cume_dist").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

valid = (
    lines
    .filter(F.col("plan").isin(["pro", "enterprise", "basic"]))
    .withColumn("tier_value_num", F.col("tier_value").cast("double"))
)

with_windows = valid.select("*", F.avg("tier_value").over(w).alias("currency_avg_tier_value"))

base = (
    with_windows
    .filter(~F.col("currency").isin(["EUR", "USD"]))
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_num",
        "currency_avg_tier_value",
        F.round((F.col("tier_value") * 1.07), 2).alias("adjusted_tier_value"),
        F.lower("plan").alias("plan_lower"),
    )
)

with_windows_price_tiers = (
    base
    .select(
        "*",
        F.count("adjusted_tier_value").over(w2).alias("currency_count_adjusted_tier_value"),
        F.dense_rank().over(w3).alias("adjusted_tier_value_dense_rank"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist"),
    )
)

filtered = (
    with_windows_price_tiers
    .filter(F.col("currency") != "THB")
    .withColumn("adjusted_tier_value_rounded", F.round("adjusted_tier_value", 1))
)

metrics = (
    filtered
    .select(
        "tier_value",
        "tier_value_num",
        "currency_avg_tier_value",
        "currency_count_adjusted_tier_value",
        "adjusted_tier_value_dense_rank",
        F.count("adjusted_tier_value_dense_rank").over(w2).alias("currency_count_adjusted_tier_value_dense_rank"),
        F.rank().over(w5).alias("adjusted_tier_value_rank"),
        F.rank().over(w6).alias("tier_value_cume_dist_rank"),
    )
)

result = (
    metrics
    .select(
        "tier_value_num",
        "adjusted_tier_value_dense_rank",
        "currency_count_adjusted_tier_value_dense_rank",
        "adjusted_tier_value_rank",
        "tier_value_cume_dist_rank",
    )
)
