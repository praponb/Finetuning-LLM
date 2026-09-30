from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

flattened = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("tier_value"))

filtered = (
    flattened
    .filter(F.col("tier_value").isNotNull())
    .select("*", F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"))
)

cleaned = (
    filtered
    .filter(~F.col("plan").isin(["enterprise", "starter", "pro"]))
    .select(
        "*",
        F.initcap(F.trim("plan")).alias("plan_title"),
        (F.col("tier_value_filled") + F.col("tier_value")).alias("tier_value_filled_plus_tier_value"),
    )
)

scored = (
    cleaned
    .select("id", "plan", F.min("tier_value").over(w).alias("plan_min_tier_value"))
)

enriched = (
    scored
    .filter(F.col("plan_min_tier_value") > 49)
    .select(
        "id",
        "plan",
        F.sum("plan_min_tier_value").over(w).alias("plan_sum_plan_min_tier_value"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("plan_sum_plan_min_tier_value") >= 359, "HIGH").otherwise("LOW").alias("plan_sum_plan_min_tier_value_tier"),
        "plan",
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sum(F.when(F.col("plan") == "basic", F.col("plan_sum_plan_min_tier_value"))).alias("basic_total_plan_sum_plan_min_tier_value"),
    )
)
