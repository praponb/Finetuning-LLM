from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.orderBy("tier_value")

lines = df_price_tiers.select("id", F.explode("discounts").alias("tier_value"))

result = (
    lines
    .filter(F.col("tier_value") >= 34)
    .select(
        "id",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
    )
)
