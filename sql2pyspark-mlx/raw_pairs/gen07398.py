from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc())
w3 = Window.orderBy(F.col("tier_value").desc_nulls_first())

flattened = df_price_tiers.select("id", F.explode_outer("discounts").alias("tier_value"))

result = (
    flattened
    .select(
        "id",
        "tier_value",
        F.rank().over(w_hist).alias("tier_value_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.percent_rank().over(w_hist).alias("tier_value_percent_rank"),
        F.percent_rank().over(w_hist).alias("tier_value_percent_rank_2"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank_2"),
    )
)
