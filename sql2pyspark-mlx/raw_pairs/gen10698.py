from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value_percent_rank").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value_rank").desc_nulls_last())
w5 = Window.orderBy("tier_value_rank")

exploded = df_price_tiers.select("id", F.explode("tier_prices").alias("tier_value"))

metrics = (
    exploded
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "tier_value_percent_rank",
        F.percent_rank().over(w3).alias("tier_value_percent_rank_percent_rank"),
        F.rank().over(w4).alias("tier_value_rank_rank"),
        F.cume_dist().over(w4).alias("tier_value_rank_cume_dist"),
        F.rank().over(w5).alias("tier_value_rank_rank_2"),
        F.cume_dist().over(w4).alias("tier_value_rank_cume_dist_2"),
    )
)

filtered = (
    with_windows
    .filter(F.col("tier_value_rank_cume_dist_2") > 0.38)
    .select(
        "id",
        "tier_value_percent_rank",
        "tier_value_percent_rank_percent_rank",
        "tier_value_rank_rank_2",
        "tier_value_rank_cume_dist_2",
        F.round((F.col("tier_value_rank_cume_dist") * 1.15), 2).alias("tier_value_rank_cume_dist_adjusted"),
    )
)

valid = (
    filtered
    .filter(F.col("tier_value_rank_cume_dist_adjusted").between(24, 42))
    .withColumn("tier_value_percent_rank_times_tier_value_percent_rank_percent_rank", (F.col("tier_value_percent_rank") * F.col("tier_value_percent_rank_percent_rank")))
)

filtered_price_tiers = (
    valid
    .filter(F.col("tier_value_rank_cume_dist_adjusted") >= 25)
    .select(
        "*",
        F.floor("tier_value_percent_rank_times_tier_value_percent_rank_percent_rank").alias("tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_whole"),
        (F.col("tier_value_rank_rank_2") + F.col("tier_value_percent_rank")).alias("tier_value_rank_rank_2_plus_tier_value_percent_rank"),
    )
)

summary = (
    filtered_price_tiers
    .groupBy(
        F.when(F.col("tier_value_rank_cume_dist_adjusted") >= 68, "HIGH").otherwise("LOW").alias("tier_value_rank_cume_dist_adjusted_tier"),
        F.when(F.col("tier_value_percent_rank_times_tier_value_percent_rank_percent_rank") >= 70, "HIGH").otherwise("LOW").alias("tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_whole", [0.25, 0.5, 0.75]).alias("tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_whole_quartiles"),
        F.count("*").alias("row_count"),
        F.avg("tier_value_percent_rank").alias("avg_tier_value_percent_rank"),
    )
)

result = (
    summary
    .select(
        "tier_value_rank_cume_dist_adjusted_tier",
        "tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_tier",
        "row_count",
        "avg_tier_value_percent_rank",
    )
    .orderBy(F.col("tier_value_rank_cume_dist_adjusted_tier").desc())
)
