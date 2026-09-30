from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").desc())
w3 = Window.orderBy(F.col("tier_value_rank").asc_nulls_last())
w4 = Window.orderBy(F.col("tier_value_times_tier_value_rank").desc())
w5 = Window.orderBy("tier_value_rank")

lines = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

cleaned = (
    lines
    .filter(F.col("plan") == "starter")
    .withColumn("tier_value_level", F.when(F.col("tier_value") >= 68, "High").when(F.col("tier_value") >= 34, "Medium").otherwise("Low"))
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 78, "High").when(F.col("tier_value") >= 40, "Medium").otherwise("Low"))
)

filtered = (
    cleaned
    .filter(F.col("plan").isin(["basic", "pro"]))
    .select(
        "*",
        F.when(F.col("tier_value") >= 64, "High").when(F.col("tier_value") >= 29, "Medium").otherwise("Low").alias("tier_value_level_2"),
    )
)

base = (
    filtered
    .filter(F.col("plan").isin(["starter", "enterprise", "basic"]))
    .withColumn("is_enterprise", F.when(F.col("plan") == "enterprise", 1).otherwise(0))
)

with_windows = (
    base
    .select("tier_value", "tier_value_level", F.rank().over(w).alias("tier_value_rank"))
)

valid = (
    with_windows
    .filter(F.col("tier_value") > 45)
    .withColumn("tier_value_times_tier_value_rank", (F.col("tier_value") * F.col("tier_value_rank")))
)

enriched = (
    valid
    .filter(F.col("tier_value").between(56, 62))
    .select(
        "tier_value_rank",
        "tier_value_times_tier_value_rank",
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.dense_rank().over(w3).alias("tier_value_rank_dense_rank"),
        F.cume_dist().over(w4).alias("tier_value_times_tier_value_rank_cume_dist"),
        F.dense_rank().over(w5).alias("tier_value_rank_dense_rank_2"),
    )
)

result = (
    enriched
    .select(
        "tier_value_times_tier_value_rank",
        "tier_value_cume_dist",
        "tier_value_rank_dense_rank",
        "tier_value_times_tier_value_rank_cume_dist",
    )
    .orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
)
