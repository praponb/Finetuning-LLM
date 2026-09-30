from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy(F.col("is_eur").desc())
w_running = Window.partitionBy("currency", "plan")
w_hist_2 = Window.partitionBy("plan", "currency").orderBy(F.col("is_eur").desc_nulls_first())
w_hist_3 = Window.partitionBy("currency")
w5 = Window.orderBy(F.col("is_eur_dense_rank").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

valid = (
    lines
    .filter(F.col("val") <= 29)
    .select("*", F.lower("plan").alias("plan_lower"))
)

valid_price_tiers = (
    valid
    .filter(F.col("val") >= 60)
    .select("*", F.when(F.col("currency") == "EUR", 1).otherwise(0).alias("is_eur"))
)

scored = (
    valid_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
    .select(
        "id",
        "val",
        "plan_lower",
        F.cume_dist().over(w_hist).alias("is_eur_cume_dist"),
        F.stddev_samp("val").over(w_running).alias("currency_stddev_samp_val"),
        F.dense_rank().over(w_hist_2).alias("is_eur_dense_rank"),
        F.avg("is_eur").over(w_hist_3).alias("currency_avg_is_eur"),
    )
)

with_windows = scored.withColumn("is_eur_dense_rank_cume_dist", F.cume_dist().over(w5))

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("is_eur_dense_rank_cume_dist") >= 0.87, "HIGH").otherwise("LOW").alias("is_eur_dense_rank_cume_dist_tier"),
        F.when(F.col("is_eur_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("is_eur_dense_rank_tier"),
        F.when(F.col("is_eur_cume_dist") >= 0.69, "HIGH").otherwise("LOW").alias("is_eur_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.percentile_approx("is_eur_dense_rank", [0.25, 0.5, 0.75]).alias("is_eur_dense_rank_quartiles"),
        F.percentile_approx("is_eur_dense_rank_cume_dist", [0.25, 0.5, 0.75]).alias("is_eur_dense_rank_cume_dist_quartiles"),
    )
)

result = (
    grouped
    .select(
        "is_eur_dense_rank_cume_dist_tier",
        "is_eur_dense_rank_tier",
        "is_eur_cume_dist_tier",
        "cnt",
        "cnt_2",
        "is_eur_dense_rank_quartiles",
        "is_eur_dense_rank_cume_dist_quartiles",
    )
    .orderBy(F.col("is_eur_dense_rank_cume_dist_tier").asc_nulls_last())
)
