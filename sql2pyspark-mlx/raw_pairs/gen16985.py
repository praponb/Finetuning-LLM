from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("days_since_effective_from").desc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy(F.year("effective_from"), "plan")

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

base = (
    flattened
    .filter((F.col("effective_from") >= F.lit("2024-03-24").cast("date")) & (F.col("element") <= 63))
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
)

valid = (
    base
    .filter(F.col("days_since_effective_from") >= 60)
    .withColumn("effective_from_plus_months", F.add_months("effective_from", 12))
)

with_windows = (
    valid
    .select(
        "plan",
        "element",
        "days_since_effective_from",
        "effective_from_plus_months",
        F.cume_dist().over(w).alias("days_since_effective_from_cume_dist"),
        F.min("days_since_effective_from").over(w2).alias("plan_min_days_since_effective_from"),
        F.avg("element").over(w3).alias("effective_from_year_avg_element"),
    )
)

kept_keys = (
    with_windows
    .select("plan")
    .intersect(
        df_price_tiers
        .filter(F.col("currency").isin(["THB", "USD"]))
        .select("plan")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("QUARTER", "effective_from_plus_months").alias("quarter_start"),
        "plan",
    )
    .agg(
        F.round(F.sum("effective_from_year_avg_element") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("effective_from_year_avg_element_per_starter"),
        F.max(F.when(F.col("plan") == "starter", F.col("days_since_effective_from_cume_dist"))).alias("starter_max_days_since_effective_from_cume_dist"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("days_since_effective_from_cume_dist", [0.25, 0.5, 0.75]).alias("days_since_effective_from_cume_dist_quartiles"),
    )
)
