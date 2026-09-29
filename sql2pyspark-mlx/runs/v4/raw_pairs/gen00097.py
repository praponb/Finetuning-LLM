from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_first())
w4 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("tier_value_cume_dist"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist_2"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist_3"),
    )
)
