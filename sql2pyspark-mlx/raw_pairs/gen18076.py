from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("avg_tier_price", "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["USD", "THB"]))
    .select(
        "id",
        "currency",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("avg_tier_price") <= 27)
    .select(
        "id",
        "currency",
        F.lag("entry_price").over(w).alias("prev_entry_price"),
        F.lag("avg_tier_price").over(w).alias("prev_avg_tier_price"),
        F.lead("avg_tier_price").over(w).alias("next_avg_tier_price"),
        F.row_number().over(w).alias("seq_no"),
    )
)
