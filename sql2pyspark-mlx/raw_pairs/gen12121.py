from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy("lowest_price", "id")
w2 = Window.partitionBy("currency").orderBy(F.col("lowest_price").desc(), "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "lowest_price",
        "avg_tier_price",
        F.lead("lowest_price").over(w_hist).alias("next_lowest_price"),
        F.lead("avg_tier_price").over(w_hist).alias("next_avg_tier_price"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
