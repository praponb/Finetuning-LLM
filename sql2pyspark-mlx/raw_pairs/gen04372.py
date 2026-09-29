from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id")
w2 = Window.partitionBy("currency").orderBy("avg_tier_price", "id")
w3 = Window.partitionBy("currency").orderBy(F.col("entry_price").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .filter(F.col("avg_tier_price") < 31)
    .select(
        "id",
        "effective_from",
        "currency",
        F.ntile(4).over(w).alias("entry_price_bucket"),
        F.lead("avg_tier_price", 2).over(w2).alias("next_avg_tier_price"),
        F.percent_rank().over(w3).alias("entry_price_percent_rank"),
        F.last("entry_price", ignorenulls=True).over(w4).alias("last_known_entry_price"),
        F.sum("entry_price").over(w5).alias("running_entry_price"),
    )
)
