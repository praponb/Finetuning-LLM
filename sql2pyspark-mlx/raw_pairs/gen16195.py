from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "effective_from"), "plan")
w2 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

filtered = (
    flattened
    .filter(F.col("plan").isin(["basic", "pro", "starter"]))
    .select("*", F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"))
)

with_windows = (
    filtered
    .select(
        "id",
        "plan",
        F.sum("tier_value_filled").over(w).alias("effective_from_period_sum_tier_value_filled"),
        F.min("tier_value").over(w2).alias("plan_min_tier_value"),
    )
)

remaining_keys = (
    with_windows
    .select("plan")
    .exceptAll(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "EUR"]))
        .select("plan")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("id", "plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from") <= "2023-05-17")
        .select("id", "plan")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "effective_from_period_sum_tier_value_filled", "plan_min_tier_value")
)
