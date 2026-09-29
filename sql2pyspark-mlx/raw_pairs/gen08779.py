from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 80, "HIGH").otherwise("LOW").alias("tier_value_tier"),
        "plan",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("cnt") > 1)
)
