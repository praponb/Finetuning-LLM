from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("effective_from"), "plan")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    lines
    .filter(F.col("tier_value") < 53)
    .select(
        "*",
        F.sum("tier_value").over(w).alias("effective_from_month_sum_tier_value"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.max("tier_value").over(w3).alias("plan_max_tier_value"),
        F.min("tier_value").over(w3).alias("plan_min_tier_value"),
    )
)
