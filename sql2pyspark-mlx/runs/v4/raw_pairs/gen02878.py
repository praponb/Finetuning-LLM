from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())
w2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    lines
    .select(
        "*",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
    )
)
