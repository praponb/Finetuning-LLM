from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

result = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("element_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.min("element").over(w3).alias("currency_min_element"),
        F.count("element").over(w3).alias("currency_count_element"),
        F.avg("element").over(w3).alias("currency_avg_element"),
    )
)
