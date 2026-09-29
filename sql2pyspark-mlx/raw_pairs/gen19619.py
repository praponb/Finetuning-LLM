from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.orderBy("element")

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("element"))
)

result = (
    lines
    .select(
        "effective_from",
        "element",
        F.rank().over(w).alias("element_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
    )
)
