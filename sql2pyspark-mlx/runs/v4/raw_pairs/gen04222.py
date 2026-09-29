from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.count("avg_tier_price").over(w_ordered).alias("plan_count_avg_tier_price"),
        F.stddev_samp("entry_price").over(w_ordered).alias("plan_stddev_samp_entry_price"),
        F.avg("avg_tier_price").over(w2).alias("avg_tier_price_avg_5"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
