from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("val").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("val"))
)

result = (
    lines
    .select(
        "*",
        F.stddev_samp("val").over(w).alias("currency_stddev_samp_val"),
        F.rank().over(w2).alias("val_rank"),
    )
)
