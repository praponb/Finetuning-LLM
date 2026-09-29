from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("element"))

result = (
    flattened
    .groupBy(
        F.when(F.col("element") >= 86, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.percentile_approx("element", 0.95).alias("element_p95"),
    )
    .filter(F.col("element_p95") >= 0)
)
