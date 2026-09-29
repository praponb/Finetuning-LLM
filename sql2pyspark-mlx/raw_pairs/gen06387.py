from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("element")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w3 = Window.orderBy(F.col("element").desc())
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

result = (
    exploded
    .select(
        "*",
        F.rank().over(w).alias("element_rank"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.cume_dist().over(w3).alias("element_cume_dist_2"),
        F.min("element").over(w4).alias("plan_min_element"),
        F.rank().over(w5).alias("element_rank_2"),
    )
)
