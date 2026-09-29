from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy("val")
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy(F.col("currency_sum_val").desc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("val_cume_dist").asc_nulls_last())
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("currency").orderBy(F.col("currency_sum_val_percent_rank").asc_nulls_first())
w8 = Window.partitionBy("currency").orderBy(F.col("val").desc())
w9 = Window.partitionBy("plan").orderBy(F.col("currency_sum_val").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

scored = (
    lines
    .withColumn("val_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
    .withColumn("currency_sum_val", F.sum("val").over(w3))
)

metrics = (
    scored
    .select(
        "*",
        F.percent_rank().over(w4).alias("currency_sum_val_percent_rank"),
        F.rank().over(w5).alias("val_cume_dist_rank"),
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w6)), 4).alias("val_dense_rank_share"),
    )
)

metrics_price_tiers = (
    metrics
    .select(
        "*",
        F.rank().over(w7).alias("currency_sum_val_percent_rank_rank"),
        F.rank().over(w8).alias("val_rank"),
        F.sum("val_cume_dist").over(w3).alias("currency_sum_val_cume_dist"),
    )
)

enriched = (
    metrics_price_tiers
    .select(
        "*",
        F.stddev_samp("currency_sum_val").over(w6).alias("plan_stddev_samp_currency_sum_val"),
        F.percent_rank().over(w9).alias("currency_sum_val_percent_rank_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("currency_sum_val_percent_rank") <= 0.68)
    .select(
        "id",
        "plan",
        "currency",
        "val_cume_dist",
        "val_dense_rank",
        "val_dense_rank_share",
        "currency_sum_val_percent_rank_rank",
        "val_rank",
        "currency_sum_val_cume_dist",
        "plan_stddev_samp_currency_sum_val",
        "currency_sum_val_percent_rank_2",
        F.when(F.col("currency_sum_val_percent_rank_rank") >= 7, "High").when(F.col("currency_sum_val_percent_rank_rank") >= 3, "Medium").otherwise("Low").alias("currency_sum_val_percent_rank_rank_band"),
        F.ceil("currency_sum_val_percent_rank").alias("currency_sum_val_percent_rank_whole"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("val_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("val_rank_tier"),
    )
    .agg(
        F.percentile_approx("currency_sum_val_percent_rank_rank", 0.9).alias("currency_sum_val_percent_rank_rank_p90"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
    .orderBy(F.col("currency_sum_val_percent_rank_rank_p90").desc_nulls_first())
)
