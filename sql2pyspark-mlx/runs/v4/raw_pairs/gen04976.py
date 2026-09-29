from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.orderBy(F.col("tier_value_times_tier_value_cume_dist").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value_cume_dist").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

with_windows = (
    lines
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
)

valid = (
    with_windows
    .filter((F.col("effective_from") > F.lit("2024-07-27").cast("date")) & (F.col("effective_from") < "2023-03-01"))
    .select(
        "*",
        (F.col("tier_value") * F.col("tier_value_cume_dist")).alias("tier_value_times_tier_value_cume_dist"),
    )
)

metrics = (
    valid
    .withColumn("tier_value_times_tier_value_cume_dist_dense_rank", F.dense_rank().over(w3))
    .withColumn("tier_value_cume_dist_percent_rank", F.percent_rank().over(w4))
)

filtered = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-10-21").cast("date"))
    .select(
        "*",
        F.round((F.col("tier_value_percent_rank") / F.col("tier_value_times_tier_value_cume_dist_dense_rank")), 2).alias("tier_value_percent_rank_per_tier_value_times_tier_value_cume_dist_dense_rank"),
        F.round((F.col("tier_value_times_tier_value_cume_dist_dense_rank") * 1.15), 2).alias("adjusted_tier_value_times_tier_value_cume_dist_dense_rank"),
    )
)

remaining_keys = (
    filtered
    .select("currency")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
        .select("currency")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency") == "THB")
        .select("id")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("adjusted_tier_value_times_tier_value_cume_dist_dense_rank") >= 83, "LARGE").otherwise("SMALL").alias("adjusted_tier_value_times_tier_value_cume_dist_dense_rank_tier"),
    )
    .agg(
        F.sum("tier_value_times_tier_value_cume_dist").alias("total_tier_value_times_tier_value_cume_dist"),
        F.percentile_approx("tier_value_times_tier_value_cume_dist_dense_rank", [0.25, 0.5, 0.75]).alias("tier_value_times_tier_value_cume_dist_dense_rank_quartiles"),
        F.percentile_approx("tier_value_cume_dist_percent_rank", 0.5).alias("tier_value_cume_dist_percent_rank_p50"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.avg("tier_value").alias("avg_tier_value"),
    )
)
