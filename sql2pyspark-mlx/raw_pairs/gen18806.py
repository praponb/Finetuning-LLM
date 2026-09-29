from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

filtered = (
    flattened
    .filter(F.col("plan") == "pro")
    .withColumn("currency_lower", F.lower("currency"))
)

base = (
    filtered
    .filter(~F.col("plan").isin(["starter", "basic", "pro"]))
    .select("*", F.lower("plan").alias("plan_lower"))
)

filtered_price_tiers = (
    base
    .filter(F.col("plan") != "basic")
    .select(
        "*",
        F.when(F.col("tier_value") >= 79, "High").when(F.col("tier_value") >= 22, "Medium").otherwise("Low").alias("tier_value_level"),
    )
)

scored = (
    filtered_price_tiers
    .select(
        "tier_value",
        "plan_lower",
        "tier_value_level",
        F.stddev_samp("tier_value").over(w).alias("currency_stddev_samp_tier_value"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.avg("tier_value").over(w3).alias("plan_avg_tier_value"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("plan_avg_tier_value") >= 85, "HIGH").otherwise("LOW").alias("plan_avg_tier_value_tier"),
    )
    .agg(
        F.round(F.avg("plan_avg_tier_value"), 2).alias("avg_plan_avg_tier_value"),
        F.min("tier_value_percent_rank").alias("min_tier_value_percent_rank"),
        F.count("*").alias("event_count"),
    )
    .orderBy("plan_avg_tier_value_tier")
)
