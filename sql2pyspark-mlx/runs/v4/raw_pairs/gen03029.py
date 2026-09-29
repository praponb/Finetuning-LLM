from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w_grp = Window.partitionBy("currency").orderBy(F.col("element").desc())
w3 = Window.orderBy(F.col("element_dense_rank").asc_nulls_last())
w4 = Window.partitionBy("currency", "plan")
w5 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w6 = Window.orderBy(F.col("element").desc_nulls_first())
w7 = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_element").desc())
w8 = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_element").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

scored = (
    lines
    .withColumn("currency_stddev_samp_element", F.stddev_samp("element").over(w))
    .withColumn("element_dense_rank", F.dense_rank().over(w_grp))
)

metrics = (
    scored
    .withColumn("element_dense_rank_percent_rank", F.percent_rank().over(w3))
    .withColumn("element_dense_rank_share", F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w4)), 4))
    .withColumn("element_percent_rank", F.percent_rank().over(w5))
    .withColumn("currency_stddev_samp_element_2", F.stddev_samp("element").over(w4))
)

result = (
    metrics
    .withColumn("element_rank", F.rank().over(w6))
    .withColumn("currency_stddev_samp_element_cume_dist", F.cume_dist().over(w7))
    .withColumn("currency_stddev_samp_element_dense_rank", F.dense_rank().over(w8))
    .withColumn("element_dense_rank_2", F.dense_rank().over(w_grp))
)
