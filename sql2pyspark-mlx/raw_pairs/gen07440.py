from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("effective_from", "id").rowsBetween(-2, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") <= "2024-10-11")
    .select(
        "id",
        "effective_from",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("entry_price").between(29, 30))
    .select(
        "id",
        "effective_from",
        "entry_price",
        F.avg("entry_price").over(w).alias("last3_avg_entry_price"),
        F.min("entry_price").over(w).alias("last3_min_entry_price"),
        F.max("entry_price").over(w).alias("last3_max_entry_price"),
    )
)
