from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())
w2 = Window.orderBy(F.col("element").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("element"))
)

result = (
    lines
    .select(
        "id",
        "effective_from",
        "element",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.rank().over(w2).alias("element_rank"),
    )
)
