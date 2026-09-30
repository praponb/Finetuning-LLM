from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy(F.month("effective_from"))
w_recent = Window.orderBy("tier_value")

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    flattened
    .select(
        "id",
        "effective_from",
        "tier_value",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.sum("tier_value").over(w2).alias("effective_from_month_sum_tier_value"),
        F.rank().over(w_recent).alias("tier_value_rank"),
    )
)
