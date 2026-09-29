from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("element"))

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 84, "HIGH").otherwise("LOW").alias("element_tier"),
        "plan",
    )
    .agg(
        F.percentile_approx("element", 0.9).alias("element_p90"),
        F.max(F.when(F.col("plan") == "starter", F.col("element"))).alias("starter_max_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("element_per_enterprise"),
    )
    .filter(F.col("starter_max_element") > 0)
)
