from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("effective_from", "id")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("entry_price").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(-6, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") < "2024-03-20")
    .select(
        "id",
        "plan",
        "effective_from",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("lowest_price") < 12)
    .select(
        "*",
        F.lead("entry_price", 1).over(w).alias("next_entry_price"),
        F.sum("entry_price").over(w2).alias("plan_sum_entry_price"),
        F.percent_rank().over(w3).alias("entry_price_percent_rank"),
        F.avg("lowest_price").over(w4).alias("lowest_price_avg_6"),
    )
)
