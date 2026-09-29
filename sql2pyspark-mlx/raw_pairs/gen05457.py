from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_limits").alias("element"))
)

result = (
    exploded
    .select(
        "id",
        "currency",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.rank().over(w).alias("element_rank"),
        F.sum("element").over(w2).alias("currency_sum_element"),
    )
)
