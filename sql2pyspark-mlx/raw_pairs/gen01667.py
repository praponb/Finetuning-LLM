from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value_int").desc())
w2 = Window.partitionBy("plan")

lines = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("tier_value"))

valid = (
    lines
    .filter(F.col("plan").isin(["enterprise", "basic", "starter"]))
    .select(
        "*",
        F.col("tier_value").cast("int").alias("tier_value_int"),
        F.lower("plan").alias("plan_lower"),
    )
)

matched_keys = (
    valid
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan").isin(["pro", "basic"]))
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("plan") == "pro")
    .select(
        "plan",
        "tier_value",
        "tier_value_int",
        "plan_lower",
        F.trim("plan").alias("clean_plan"),
    )
)

remaining_keys = (
    base
    .select("plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") != "USD")
        .select("plan")
    )
)

kept_rows_price_tiers = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows_price_tiers
    .filter(F.col("plan") != "pro")
    .select(
        "plan",
        "tier_value_int",
        F.percent_rank().over(w).alias("tier_value_int_percent_rank"),
        F.sum("tier_value").over(w2).alias("plan_sum_tier_value"),
        F.min("tier_value").over(w2).alias("plan_min_tier_value"),
        F.sum("tier_value_int").over(w2).alias("plan_sum_tier_value_int"),
    )
)

result = (
    metrics
    .cube(
        "plan",
        F.when(F.col("plan_sum_tier_value_int") >= 326, "PREMIUM").otherwise("STANDARD").alias("plan_sum_tier_value_int_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_int", [0.25, 0.5, 0.75]).alias("tier_value_int_quartiles"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
    )
)
