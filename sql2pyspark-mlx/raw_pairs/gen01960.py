from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("element"))
)

result = (
    lines
    .select(
        "id",
        "currency",
        F.rank().over(w).alias("element_rank"),
        F.stddev_samp("element").over(w2).alias("currency_stddev_samp_element"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
    )
)
