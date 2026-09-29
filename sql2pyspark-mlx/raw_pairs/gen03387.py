from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(-5, Window.currentRow)
w_grp = Window.partitionBy("currency")
w_grp_2 = Window.partitionBy(F.year("effective_from"), "plan").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.year("effective_from"), "currency").orderBy("entry_price", "id").rowsBetween(-6, -1)

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from").between("2024-12-31", "2025-08-30"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .withColumn("entry_price_avg_5", F.avg("entry_price").over(w))
    .withColumn("currency_stddev_samp_avg_tier_price", F.stddev_samp("avg_tier_price").over(w_grp))
    .withColumn("first_known_avg_tier_price", F.first("avg_tier_price", ignorenulls=True).over(w_grp_2))
    .withColumn("entry_price_avg_6_prior", F.avg("entry_price").over(w_hist))
)
