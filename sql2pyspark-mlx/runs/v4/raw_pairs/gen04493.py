from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_part = Window.orderBy(F.col("days_effective_from_minus_31d_to_effective_from").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

cleaned = (
    lines
    .filter(F.col("currency") == "EUR")
    .withColumn("effective_from_minus_31d", F.date_sub("effective_from", 31))
    .withColumn("effective_from_year", F.year("effective_from"))
)

filtered = (
    cleaned
    .filter(F.col("effective_from") > "2025-01-01")
    .withColumn("effective_from_year_tier", F.when(F.col("effective_from_year") >= 79, "High").when(F.col("effective_from_year") >= 28, "Medium").otherwise("Low"))
    .withColumn("tier_value_str", F.col("tier_value").cast("string"))
)

base = (
    filtered
    .filter(F.col("effective_from") >= F.lit("2024-06-02").cast("date"))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_value",
        "effective_from_year_tier",
        "tier_value_str",
        F.datediff("effective_from", "effective_from_minus_31d").alias("days_effective_from_minus_31d_to_effective_from"),
    )
)

scored = (
    base
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("days_effective_from_minus_31d_to_effective_from_cume_dist", F.cume_dist().over(w_part))
)

base_price_tiers = (
    scored
    .filter(F.col("days_effective_from_minus_31d_to_effective_from_cume_dist") <= 0.44)
    .select(
        "effective_from",
        "currency",
        "tier_value",
        "effective_from_year_tier",
        "tier_value_str",
        "days_effective_from_minus_31d_to_effective_from",
        "tier_value_rank",
        "days_effective_from_minus_31d_to_effective_from_cume_dist",
        F.last_day("effective_from").alias("effective_from_month_end"),
    )
)

result = (
    base_price_tiers
    .select(
        "currency",
        "effective_from_year_tier",
        "tier_value_str",
        "days_effective_from_minus_31d_to_effective_from",
        "tier_value_rank",
        "days_effective_from_minus_31d_to_effective_from_cume_dist",
        "effective_from_month_end",
    )
)
