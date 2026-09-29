from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency")
w_recent = Window.partitionBy("currency").orderBy(F.col("val").desc())

lines = df_price_tiers.select("id", "currency", F.explode_outer("discounts").alias("val"))

result = (
    lines
    .select(
        "id",
        "currency",
        F.sum("val").over(w_hist).alias("currency_sum_val"),
        F.sum("val").over(w_hist).alias("currency_sum_val_2"),
        F.rank().over(w_recent).alias("val_rank"),
        F.stddev_samp("val").over(w_hist).alias("currency_stddev_samp_val"),
        F.sum("val").over(w_hist).alias("currency_sum_val_3"),
    )
)
