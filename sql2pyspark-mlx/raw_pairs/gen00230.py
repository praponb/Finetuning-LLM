from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w_running = Window.partitionBy(F.date_trunc("month", "effective_from"), "currency")
w3 = Window.partitionBy(F.month("effective_from"), "currency").orderBy(F.col("tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "currency",
        "tier_value",
        F.sum("tier_value").over(w).alias("currency_sum_tier_value"),
        F.min("tier_value").over(w_running).alias("effective_from_period_min_tier_value"),
        F.rank().over(w3).alias("tier_value_rank"),
    )
)
