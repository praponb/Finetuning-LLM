from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w5 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    flattened
    .select(
        "plan",
        "currency",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank_2"),
        F.dense_rank().over(w4).alias("tier_value_dense_rank_2"),
        F.sum("tier_value").over(w5).alias("plan_sum_tier_value"),
    )
)
