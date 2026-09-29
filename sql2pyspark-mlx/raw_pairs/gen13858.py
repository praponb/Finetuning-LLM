from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value_dense_rank").desc())
w4 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_running = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("tier_value_rank").asc_nulls_last())
w7 = Window.orderBy(F.col("tier_value_dense_rank_dense_rank").desc())
w8 = Window.orderBy(F.col("tier_value_dense_rank_rank").desc_nulls_first())
w9 = Window.partitionBy("tier_value_dense_rank_rank_percent_rank_tier").orderBy(F.col("tier_value_cume_dist_dense_rank_p50").desc())

flattened = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("tier_value"))

with_windows = (
    flattened
    .select(
        "id",
        "tier_value",
        F.rank().over(w).alias("tier_value_rank"),
        F.rank().over(w).alias("tier_value_rank_2"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)

remaining_keys = (
    with_windows
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "THB"]))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_price_tiers = (
    verified
    .select(
        "id",
        "tier_value",
        "tier_value_rank",
        "tier_value_rank_2",
        "tier_value_cume_dist",
        F.rank().over(w3).alias("tier_value_dense_rank_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank_dense_rank"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist_2"),
    )
)

scored = (
    with_windows_price_tiers
    .filter(F.col("tier_value_rank") != 6)
    .select(
        "id",
        "tier_value_rank",
        "tier_value_rank_2",
        "tier_value_dense_rank_rank",
        "tier_value_cume_dist_2",
        F.dense_rank().over(w4).alias("tier_value_dense_rank"),
        F.dense_rank().over(w_running).alias("tier_value_cume_dist_dense_rank"),
        F.dense_rank().over(w6).alias("tier_value_rank_dense_rank"),
        F.cume_dist().over(w7).alias("tier_value_dense_rank_dense_rank_cume_dist"),
        F.percent_rank().over(w8).alias("tier_value_dense_rank_rank_percent_rank"),
    )
)

valid = (
    scored
    .filter(F.col("tier_value_rank") == 4)
    .select(
        "id",
        "tier_value_rank",
        "tier_value_rank_2",
        "tier_value_dense_rank",
        "tier_value_cume_dist_dense_rank",
        "tier_value_dense_rank_rank_percent_rank",
        F.greatest("tier_value_dense_rank_dense_rank_cume_dist", "tier_value_rank_2").alias("greatest_tier_value_dense_rank_dense_rank_cume_dist_tier_value_rank_2"),
        (F.col("tier_value_dense_rank_rank_percent_rank") - F.col("tier_value_dense_rank_rank")).alias("tier_value_dense_rank_rank_percent_rank_minus_tier_value_dense_rank_rank"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("tier_value_dense_rank_rank_percent_rank") >= 0.73, "HIGH").otherwise("LOW").alias("tier_value_dense_rank_rank_percent_rank_tier"),
        F.when(F.col("tier_value_dense_rank_rank_percent_rank_minus_tier_value_dense_rank_rank") >= 70, "HIGH").otherwise("LOW").alias("tier_value_dense_rank_rank_percent_rank_minus_tier_value_dense_rank_rank_tier"),
        F.when(F.col("tier_value_rank_2") >= 8, "PREMIUM").otherwise("STANDARD").alias("tier_value_rank_2_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_rank", [0.25, 0.5, 0.75]).alias("tier_value_rank_quartiles"),
        F.percentile_approx("tier_value_cume_dist_dense_rank", 0.5).alias("tier_value_cume_dist_dense_rank_p50"),
        F.percentile_approx("tier_value_cume_dist_dense_rank", 0.5).alias("tier_value_cume_dist_dense_rank_p50_2"),
    )
    .withColumn("tier_value_cume_dist_dense_rank_p50_rank", F.rank().over(w9))
)
