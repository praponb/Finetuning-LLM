from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy(F.month("effective_from"), "currency").orderBy(F.col("val").desc())
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

result = (
    exploded
    .select(
        "*",
        F.sum("val").over(w).alias("currency_sum_val"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.percent_rank().over(w3).alias("val_percent_rank"),
    )
)
