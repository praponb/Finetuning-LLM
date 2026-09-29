from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("tier_value"))
)

result = (
    lines
    .filter(F.col("effective_from") < "2024-12-15")
    .select(
        "effective_from",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
    )
)
