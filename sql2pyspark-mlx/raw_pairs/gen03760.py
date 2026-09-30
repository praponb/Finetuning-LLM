from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy("tier_value")

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_prices").alias("tier_value"))
)

enriched = (
    exploded
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        F.sum("tier_value").over(w).alias("plan_sum_tier_value"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
    )
)

base = (
    enriched
    .filter(F.col("plan_sum_tier_value") < 326)
    .select("*", F.trim("plan").alias("clean_plan"))
)

filtered = (
    base
    .filter(F.col("plan_sum_tier_value") < 118)
    .withColumn("tier_value_cume_dist_filled", F.coalesce("tier_value_cume_dist", F.lit(0)))
)

cleaned = (
    filtered
    .filter(F.col("tier_value_cume_dist_filled") <= 48)
    .select(
        "*",
        F.when(F.col("tier_value_cume_dist") >= 0.61, "High").when(F.col("tier_value_cume_dist") >= 0.14, "Medium").otherwise("Low").alias("tier_value_cume_dist_level"),
    )
)

valid = (
    cleaned
    .filter(F.col("tier_value_cume_dist").between(0.39, 0.44))
    .withColumn("tier_value_cume_dist_level_2", F.when(F.col("tier_value_cume_dist") >= 0.82, "High").when(F.col("tier_value_cume_dist") >= 0.27, "Medium").otherwise("Low"))
)

cleaned_price_tiers = (
    valid
    .filter(F.col("plan_sum_tier_value") <= 158)
    .select(
        "*",
        F.col("tier_value_cume_dist_filled").cast("int").alias("tier_value_cume_dist_filled_int"),
    )
)

result = (
    cleaned_price_tiers
    .select(
        "plan_sum_tier_value",
        "tier_value_cume_dist",
        "clean_plan",
        "tier_value_cume_dist_filled",
        "tier_value_cume_dist_level",
        "tier_value_cume_dist_filled_int",
    )
    .orderBy(F.col("plan_sum_tier_value").desc(), "tier_value_cume_dist_filled_int")
)
