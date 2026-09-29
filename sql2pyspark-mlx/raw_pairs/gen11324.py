from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("plan")
w_ordered = Window.orderBy("tier_value")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("effective_from") < "2024-04-13")
    .select(
        "*",
        F.last_day("effective_from").alias("effective_from_month_end"),
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

matched_keys = (
    cleaned
    .select("id", "plan")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") == "pro")
        .select("id", "plan")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("plan") == "pro")
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.avg("tier_value").over(w2).alias("plan_avg_tier_value"),
        F.dense_rank().over(w_ordered).alias("tier_value_dense_rank"),
    )
)

cleaned_price_tiers = (
    enriched
    .filter(F.col("tier_value_percent_rank") <= 0.57)
    .select(
        "*",
        F.date_trunc("MONTH", "effective_from").alias("effective_from_period"),
        F.lower("plan").alias("plan_lower"),
    )
)

result = (
    cleaned_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "plan_title",
        "tier_value_percent_rank",
        "plan_avg_tier_value",
        "effective_from_period",
        "plan_lower",
    )
    .orderBy(F.col("effective_from_period").desc_nulls_first())
)
