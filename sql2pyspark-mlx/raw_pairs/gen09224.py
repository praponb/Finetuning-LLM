from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("val")
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy(F.date_trunc("WEEK", "effective_from"))
w4 = Window.partitionBy(F.date_trunc("WEEK", "effective_from")).orderBy(F.col("val_percent_rank").asc_nulls_last())
w5 = Window.orderBy(F.col("val_cume_dist").desc_nulls_first())
w_recent = Window.orderBy(F.col("val_percent_rank").desc_nulls_first())
w7 = Window.orderBy(F.col("effective_from_period_stddev_samp_val").desc_nulls_last())
w8 = Window.orderBy("effective_from_period_stddev_samp_val")
w9 = Window.orderBy(F.col("val").asc_nulls_last())
w_part_2 = Window.orderBy(F.col("effective_from_period_stddev_samp_val_cume_dist").desc_nulls_first())
w11 = Window.partitionBy(F.date_trunc("MONTH", "effective_from")).orderBy(F.col("effective_from_period_stddev_samp_val_cume_dist").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

enriched = (
    exploded
    .select(
        "id",
        "effective_from",
        "val",
        F.cume_dist().over(w_part).alias("val_cume_dist"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.stddev_samp("val").over(w3).alias("effective_from_period_stddev_samp_val"),
        F.cume_dist().over(w_part).alias("val_cume_dist_2"),
        F.cume_dist().over(w_part).alias("val_cume_dist_3"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w4).alias("val_percent_rank_cume_dist"),
        F.cume_dist().over(w5).alias("val_cume_dist_cume_dist"),
    )
)

enriched_price_tiers = (
    scored
    .select(
        "effective_from",
        "val",
        "val_percent_rank",
        "effective_from_period_stddev_samp_val",
        "val_cume_dist_cume_dist",
        F.dense_rank().over(w_recent).alias("val_percent_rank_dense_rank"),
        F.cume_dist().over(w7).alias("effective_from_period_stddev_samp_val_cume_dist"),
    )
)

with_windows = (
    enriched_price_tiers
    .filter(F.col("val_percent_rank_dense_rank").between(3, 8))
    .select(
        "*",
        F.percent_rank().over(w8).alias("effective_from_period_stddev_samp_val_percent_rank"),
        F.dense_rank().over(w9).alias("val_dense_rank"),
        F.rank().over(w_part_2).alias("effective_from_period_stddev_samp_val_cume_dist_rank"),
        F.rank().over(w11).alias("effective_from_period_stddev_samp_val_cume_dist_rank_2"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("effective_from_period_stddev_samp_val_cume_dist") >= 0.63, "HIGH").otherwise("LOW").alias("effective_from_period_stddev_samp_val_cume_dist_tier"),
        F.when(F.col("val_cume_dist_cume_dist") >= 0.63, "LARGE").otherwise("SMALL").alias("val_cume_dist_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max("effective_from_period_stddev_samp_val_cume_dist_rank_2").alias("max_effective_from_period_stddev_samp_val_cume_dist_rank_2"),
    )
)

result = (
    summary
    .select(
        "effective_from_period_stddev_samp_val_cume_dist_tier",
        "val_cume_dist_cume_dist_tier",
        "max_effective_from_period_stddev_samp_val_cume_dist_rank_2",
    )
    .orderBy(
        F.col("effective_from_period_stddev_samp_val_cume_dist_tier").desc_nulls_last(),
    )
)
