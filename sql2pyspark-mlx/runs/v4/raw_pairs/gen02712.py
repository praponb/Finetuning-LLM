from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w_running = Window.partitionBy("currency")
w_recent = Window.partitionBy("currency").orderBy(F.col("element_cume_dist").desc_nulls_last())
w6 = Window.orderBy(F.col("element_cume_dist").desc_nulls_last())
w7 = Window.partitionBy("currency").orderBy(F.col("element_cume_dist_rank").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_limits").alias("element"))
)

metrics = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank_2"),
        F.cume_dist().over(w3).alias("element_cume_dist"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.avg("element").over(w_running).alias("currency_avg_element"),
        F.rank().over(w_recent).alias("element_cume_dist_rank"),
        F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w_running)), 4).alias("element_dense_rank_share"),
    )
)

base = (
    with_windows
    .filter(F.col("element_cume_dist") > 0.72)
    .select(
        "*",
        (F.col("element_dense_rank_share") / F.nullif("element_cume_dist", F.lit(0))).alias("element_dense_rank_share_per_element_cume_dist"),
    )
)

scored = (
    base
    .withColumn("currency_count_element_dense_rank_2", F.count("element_dense_rank_2").over(w_running))
    .withColumn("element_cume_dist_cume_dist", F.cume_dist().over(w6))
    .withColumn("currency_stddev_samp_element_dense_rank_share", F.stddev_samp("element_dense_rank_share").over(w_running))
    .withColumn("element_cume_dist_rank_rank", F.rank().over(w7))
)

result = (
    scored
    .select(
        "currency",
        "element",
        "element_dense_rank",
        "element_dense_rank_2",
        "element_cume_dist_rank",
        "element_dense_rank_share",
        "currency_count_element_dense_rank_2",
        "element_cume_dist_cume_dist",
        "currency_stddev_samp_element_dense_rank_share",
    )
)
