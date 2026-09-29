from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_part = Window.orderBy(F.col("val").desc_nulls_last())
w_grp = Window.partitionBy("plan").orderBy(F.col("val_cume_dist").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("val_cume_dist").desc_nulls_first())
w_recent = Window.partitionBy("currency", "plan").orderBy(F.col("plan_min_val").asc_nulls_last())
w6 = Window.partitionBy("plan", "currency").orderBy(F.col("val_cume_dist_cume_dist").desc_nulls_first())
w7 = Window.partitionBy("currency")
w8 = Window.partitionBy("currency").orderBy("val_cume_dist_cume_dist_cume_dist")
w9 = Window.partitionBy("plan").orderBy("plan_min_val_dense_rank")

lines = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

scored = (
    lines
    .withColumn("plan_min_val", F.min("val").over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w_part))
)

with_windows = (
    scored
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("val_cume_dist_cume_dist"),
        F.rank().over(w4).alias("val_cume_dist_rank"),
        F.dense_rank().over(w_recent).alias("plan_min_val_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w6).alias("val_cume_dist_cume_dist_cume_dist"),
        F.count("val_cume_dist").over(w).alias("plan_count_val_cume_dist"),
    )
)

metrics_price_tiers = (
    metrics
    .withColumn("currency_count_val_cume_dist_cume_dist", F.count("val_cume_dist_cume_dist").over(w7))
    .withColumn("val_cume_dist_cume_dist_cume_dist_cume_dist", F.cume_dist().over(w8))
    .withColumn("plan_min_val_dense_rank_dense_rank", F.dense_rank().over(w9))
)

result = (
    metrics_price_tiers
    .groupBy("currency")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("plan_min_val_dense_rank_dense_rank", 0.5).alias("plan_min_val_dense_rank_dense_rank_p50"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.percentile_approx("plan_min_val_dense_rank_dense_rank", 0.5).alias("plan_min_val_dense_rank_dense_rank_p50_2"),
    )
    .orderBy(F.col("plan_min_val_dense_rank_dense_rank_p50_2").desc())
)
