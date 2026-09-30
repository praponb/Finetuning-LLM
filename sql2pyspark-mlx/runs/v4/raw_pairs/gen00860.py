from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")
w2 = Window.orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy(F.col("element").desc_nulls_first())
w4 = Window.orderBy(F.col("element_rank").asc_nulls_first())
w5 = Window.orderBy(F.col("element_percent_rank").desc_nulls_first())
w_running = Window.orderBy(F.col("element_rank").desc_nulls_first())

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("element"))

scored = (
    exploded
    .select(
        "*",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
    )
)

scored_price_tiers = (
    scored
    .withColumn("element_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("element_rank", F.rank().over(w2))
)

result = (
    scored_price_tiers
    .select(
        "*",
        F.dense_rank().over(w4).alias("element_rank_dense_rank"),
        F.percent_rank().over(w5).alias("element_percent_rank_percent_rank"),
        F.cume_dist().over(w_running).alias("element_rank_cume_dist"),
    )
)
