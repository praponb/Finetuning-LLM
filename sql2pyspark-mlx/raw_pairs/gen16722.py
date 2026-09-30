from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("plan")

flattened = df_price_tiers.select("id", "plan", F.explode("discounts").alias("tier_value"))

result = (
    flattened
    .filter(F.col("tier_value").isNotNull())
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.max("tier_value").over(w2).alias("plan_max_tier_value"),
        F.count("tier_value").over(w2).alias("plan_count_tier_value"),
    )
)
