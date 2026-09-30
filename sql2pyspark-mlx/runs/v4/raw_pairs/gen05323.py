from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("val").asc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w_ordered = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_val").asc_nulls_first())
w_running = Window.partitionBy("currency").orderBy("currency_sum_val")
w_part = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w_grp = Window.orderBy(F.col("val_cume_dist").desc_nulls_last())
w9 = Window.orderBy(F.col("val_cume_dist_2").desc_nulls_last())

exploded = df_price_tiers.select("id", "currency", F.explode_outer("tier_prices").alias("val"))

enriched = (
    exploded
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w2))
    .withColumn("currency_sum_val", F.sum("val").over(w2))
    .withColumn("val_dense_rank", F.dense_rank().over(w3))
    .withColumn("val_cume_dist", F.cume_dist().over(w4))
)

scored = (
    enriched
    .withColumn("currency_stddev_samp_val_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("currency_min_currency_stddev_samp_val", F.min("currency_stddev_samp_val").over(w2))
    .withColumn("currency_sum_val_cume_dist", F.cume_dist().over(w_running))
    .withColumn("val_cume_dist_2", F.cume_dist().over(w_part))
    .withColumn("val_cume_dist_percent_rank", F.percent_rank().over(w_grp))
)

with_windows = (
    scored
    .withColumn("val_cume_dist_2_cume_dist", F.cume_dist().over(w9))
    .withColumn("currency_max_val_cume_dist", F.max("val_cume_dist").over(w2))
    .withColumn("currency_sum_val_cume_dist_2", F.sum("val_cume_dist_2").over(w2))
)

result = (
    with_windows
    .rollup(
        F.when(F.col("currency_sum_val_cume_dist") >= 0.77, "HIGH").otherwise("LOW").alias("currency_sum_val_cume_dist_tier"),
        F.when(F.col("currency_sum_val") >= 426, "HIGH").otherwise("LOW").alias("currency_sum_val_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.sort_array(F.collect_set("currency")).alias("currency_list_2"),
    )
    .filter(F.col("num_currencies") > 1)
    .orderBy(F.col("currency_sum_val_tier").desc_nulls_first())
)
