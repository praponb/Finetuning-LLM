from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan").orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "plan",
        "effective_from",
        "currency",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.sum("tier_value").over(w2).alias("currency_sum_tier_value"),
    )
)
