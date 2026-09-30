from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("val")
w_ordered = Window.orderBy(F.col("val").desc_nulls_last())
w3 = Window.orderBy(F.col("val").asc_nulls_last())
w_grp = Window.orderBy("val_cume_dist")

exploded = df_price_tiers.select("id", F.explode("tier_limits").alias("val"))

metrics = (
    exploded
    .select(
        "id",
        "val",
        F.cume_dist().over(w_recent).alias("val_cume_dist"),
        F.rank().over(w_ordered).alias("val_rank"),
    )
)

metrics_price_tiers = (
    metrics
    .withColumn("val_percent_rank", F.percent_rank().over(w3))
    .withColumn("val_cume_dist_dense_rank", F.dense_rank().over(w_grp))
)

kept_keys = (
    metrics_price_tiers
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("effective_from") >= "2025-07-01")
        .select("id")
    )
)

verified = (
    metrics_price_tiers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") <= "2024-09-25")
        .select("id")
    )
)

verified_price_tiers = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified_price_tiers
    .filter(F.col("val_percent_rank") <= 0.66)
    .withColumn("val_percent_rank_whole", F.floor("val_percent_rank"))
    .withColumn("val_cume_dist_dense_rank_whole", F.ceil("val_cume_dist_dense_rank"))
)

result = (
    base
    .withColumn("val_cume_dist_dense_rank_tier", F.when(F.col("val_cume_dist_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .cube("val_cume_dist_dense_rank_tier")
    .agg(
        F.round(F.avg("val_percent_rank"), 2).alias("avg_val_percent_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("val_cume_dist", 0.5).alias("val_cume_dist_p50"),
        F.grouping("val_cume_dist_dense_rank_tier").alias("gid"),
    )
)
