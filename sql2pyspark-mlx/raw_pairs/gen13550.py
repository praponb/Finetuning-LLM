from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("val").desc())

flattened = df_price_tiers.select("id", "currency", F.explode_outer("discounts").alias("val"))

result = (
    flattened
    .select(
        "currency",
        "val",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.sum("val").over(w2).alias("currency_sum_val"),
        F.percent_rank().over(w3).alias("val_percent_rank_2"),
        F.dense_rank().over(w4).alias("val_dense_rank"),
        F.min("val").over(w2).alias("currency_min_val"),
    )
)
