from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

base = (
    lines
    .filter(F.col("plan") != "pro")
    .withColumn("effective_from_plus_months", F.add_months("effective_from", 3))
    .withColumn("plan_lower", F.lower("plan"))
)

valid = (
    base
    .filter(F.month("effective_from").isin([1, 11, 12]))
    .withColumn("weeks_effective_from_plus_months_to_effective_from", (F.datediff("effective_from", "effective_from_plus_months") / 7))
    .withColumn("is_basic", F.when(F.col("plan") == "basic", 1).otherwise(0))
)

cleaned = (
    valid
    .filter(F.col("effective_from") > F.lit("2024-02-26").cast("date"))
    .withColumn("val_per_weeks_effective_from_plus_months_to_effective_from", (F.col("val") / F.nullif("weeks_effective_from_plus_months_to_effective_from", F.lit(0))))
)

base_price_tiers = (
    cleaned
    .filter(F.col("effective_from") > "2023-08-11")
    .select(
        "id",
        "plan",
        "effective_from",
        "effective_from_plus_months",
        "weeks_effective_from_plus_months_to_effective_from",
        "is_basic",
        "val_per_weeks_effective_from_plus_months_to_effective_from",
        F.quarter("effective_from").alias("effective_from_quarter"),
    )
)

result = (
    base_price_tiers
    .rollup(
        F.when(F.col("val_per_weeks_effective_from_plus_months_to_effective_from") >= 72, "HIGH").otherwise("LOW").alias("val_per_weeks_effective_from_plus_months_to_effective_from_tier"),
    )
    .agg(
        F.percentile_approx("val_per_weeks_effective_from_plus_months_to_effective_from", [0.25, 0.5, 0.75]).alias("val_per_weeks_effective_from_plus_months_to_effective_from_quartiles"),
        F.percentile_approx("effective_from_quarter", 0.95).alias("effective_from_quarter_p95"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.sum(F.when(F.col("plan") == "basic", F.col("effective_from_quarter")).otherwise(0)).alias("basic_effective_from_quarter"),
        F.round(F.avg("is_basic"), 2).alias("avg_is_basic"),
        F.percentile_approx("weeks_effective_from_plus_months_to_effective_from", 0.9).alias("weeks_effective_from_plus_months_to_effective_from_p90"),
    )
)
