from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "effective_from")).orderBy("tier_value")
w2 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.partitionBy(F.date_trunc("MONTH", "effective_from"))

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("tier_value"))
)

result = (
    lines
    .select(
        "effective_from",
        "tier_value",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.rank().over(w2).alias("tier_value_rank_2"),
        F.count("tier_value").over(w3).alias("effective_from_period_count_tier_value"),
        F.rank().over(w2).alias("tier_value_rank_3"),
    )
)
