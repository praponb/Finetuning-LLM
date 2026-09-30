from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy(F.date_trunc("week", "effective_from"), "currency").orderBy("tier_value")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    flattened
    .filter(F.month("effective_from").isin([2, 10, 12]))
    .select(
        "id",
        "plan",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank_2"),
    )
)
