from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w2 = Window.orderBy(F.col("element").asc_nulls_first())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy(F.col("element_cume_dist").asc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("element_cume_dist_cume_dist").desc())
w6 = Window.orderBy(F.col("element").desc())
w7 = Window.partitionBy("currency").orderBy(F.col("element_cume_dist_cume_dist_percent_rank").desc_nulls_first())
w8 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_limits").alias("element"))
)

scored = (
    flattened
    .withColumn("element_cume_dist", F.cume_dist().over(w_running))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
    .withColumn("currency_count_element", F.count("element").over(w3))
    .withColumn("element_rank", F.rank().over(w_running))
)

scored_price_tiers = scored.select("*", F.cume_dist().over(w4).alias("element_cume_dist_cume_dist"))

with_windows = (
    scored_price_tiers
    .filter(F.col("element_rank") != 6)
    .withColumn("element_cume_dist_cume_dist_percent_rank", F.percent_rank().over(w5))
    .withColumn("element_rank_2", F.rank().over(w6))
)

scored_price_tiers_2 = (
    with_windows
    .select(
        "element_percent_rank",
        "currency_count_element",
        "element_cume_dist_cume_dist",
        "element_cume_dist_cume_dist_percent_rank",
        "element_rank_2",
        F.dense_rank().over(w7).alias("element_cume_dist_cume_dist_percent_rank_dense_rank"),
        F.round((F.col("element_rank_2") / F.sum("element_rank_2").over(w3)), 4).alias("element_rank_2_share"),
        F.stddev_samp("element_rank").over(w3).alias("currency_stddev_samp_element_rank"),
        F.dense_rank().over(w8).alias("element_dense_rank"),
        F.round((F.col("element_rank") / F.sum("element_rank").over(w3)), 4).alias("element_rank_share"),
    )
)

filtered = (
    scored_price_tiers_2
    .filter(F.col("element_cume_dist_cume_dist_percent_rank_dense_rank") > 6)
    .withColumn("element_rank_2_share_int", F.col("element_rank_2_share").cast("int"))
    .withColumn("element_dense_rank_times_element_percent_rank", (F.col("element_dense_rank") * F.col("element_percent_rank")))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("element_rank_2_share_int") >= 78, "HIGH").otherwise("LOW").alias("element_rank_2_share_int_tier"),
        F.when(F.col("currency_stddev_samp_element_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("currency_stddev_samp_element_rank_tier"),
        F.when(F.col("element_percent_rank") >= 0.61, "LARGE").otherwise("SMALL").alias("element_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("element_rank_2"), 2).alias("avg_element_rank_2"),
        F.count("*").alias("event_count"),
        F.round(F.avg("element_rank_share"), 2).alias("avg_element_rank_share"),
        F.round(F.avg("currency_stddev_samp_element_rank"), 2).alias("avg_currency_stddev_samp_element_rank"),
        F.round(F.avg("element_rank_2"), 2).alias("avg_element_rank_2_2"),
    )
)
