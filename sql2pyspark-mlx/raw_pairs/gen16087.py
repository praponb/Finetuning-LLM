from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").asc_nulls_first())

flattened = (
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
    flattened
    .filter(F.col("currency") != "THB")
    .select(
        "id",
        "plan",
        "effective_from",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.count("tier_value").over(w2).alias("plan_count_tier_value"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
    )
)
