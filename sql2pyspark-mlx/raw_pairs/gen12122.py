from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_part = Window.orderBy(F.col("tier_value").desc())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    flattened
    .select(
        "id",
        "effective_from",
        F.dense_rank().over(w_ordered).alias("tier_value_dense_rank"),
        F.dense_rank().over(w_part).alias("tier_value_dense_rank_2"),
    )
)
