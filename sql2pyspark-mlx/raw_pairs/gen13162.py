from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("avg_tier_price", "id")
w2 = Window.partitionBy("plan").orderBy("effective_from", "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") < "2025-01-01")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "plan",
        "effective_from",
        "entry_price",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w).alias("avg_tier_price_bucket"),
    )
)
