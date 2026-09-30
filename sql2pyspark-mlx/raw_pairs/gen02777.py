from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value_whole_times_tier_value").asc_nulls_first())

lines = df_price_tiers.select("id", F.explode("discounts").alias("tier_value"))

filtered = (
    lines
    .filter(F.col("tier_value") >= 31)
    .withColumn("tier_value_whole", F.floor("tier_value"))
)

base = (
    filtered
    .filter(F.col("tier_value_whole") > 24)
    .withColumn("tier_value_plus_tier_value_whole", (F.col("tier_value") + F.col("tier_value_whole")))
)

valid = (
    base
    .filter(F.col("tier_value_whole").between(24, 67))
    .withColumn("tier_value_rounded", F.round("tier_value", 0))
    .withColumn("tier_value_whole_times_tier_value", (F.col("tier_value_whole") * F.col("tier_value")))
)

scored = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_whole_times_tier_value_percent_rank"),
    )
)

base_price_tiers = (
    scored
    .filter(F.col("tier_value_rounded").isNotNull())
    .select(
        "tier_value",
        "tier_value_plus_tier_value_whole",
        "tier_value_whole_times_tier_value",
        (F.col("tier_value_rounded") - F.col("tier_value_whole")).alias("tier_value_rounded_minus_tier_value_whole"),
    )
)

result = (
    base_price_tiers
    .withColumn("tier_value_plus_tier_value_whole_tier", F.when(F.col("tier_value_plus_tier_value_whole") >= 84, "LARGE").otherwise("SMALL"))
    .withColumn("tier_value_whole_times_tier_value_tier", F.when(F.col("tier_value_whole_times_tier_value") >= 85, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["tier_value_plus_tier_value_whole_tier", "tier_value_whole_times_tier_value_tier"], ["tier_value_plus_tier_value_whole_tier"]],
        "tier_value_plus_tier_value_whole_tier",
        "tier_value_whole_times_tier_value_tier",
    )
    .agg(
        F.avg("tier_value").alias("avg_tier_value"),
        F.percentile_approx("tier_value_whole_times_tier_value", 0.95).alias("tier_value_whole_times_tier_value_p95"),
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("tier_value_p90")
)
