from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

result = (
    lines
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.rank().over(w3).alias("tier_value_rank"),
        F.max("tier_value").over(w4).alias("plan_max_tier_value"),
    )
)
