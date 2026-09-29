from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan")

parsed = (
    df_price_tiers
    .filter(F.col("plan").isin(["basic", "pro", "enterprise"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .select(
        "id",
        "plan",
        "effective_from",
        F.max("entry_price").over(w_grp).alias("running_max_entry_price"),
        F.min("entry_price").over(w_grp).alias("running_min_entry_price"),
        F.sum("entry_price").over(w_grp).alias("running_sum_entry_price"),
        F.max("avg_tier_price").over(w2).alias("plan_max_avg_tier_price"),
    )
)
