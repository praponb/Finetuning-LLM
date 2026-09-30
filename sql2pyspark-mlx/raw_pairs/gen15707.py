from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy(F.year("effective_from"))
w4 = Window.partitionBy(F.year("effective_from")).orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
w5 = Window.partitionBy("plan")
w_ordered = Window.orderBy(F.col("tier_value_cume_dist").asc_nulls_first())
w7 = Window.partitionBy("plan").orderBy(F.col("effective_from_year_stddev_samp_currency_max_tier_value").asc_nulls_last())
w8 = Window.partitionBy("plan").orderBy(F.col("currency_max_tier_value").desc())
w9 = Window.orderBy(F.col("currency_max_tier_value_rank_adjusted").asc_nulls_first())
w_part = Window.orderBy(F.col("currency_max_tier_value_rank_adjusted").desc_nulls_first())
w11 = Window.orderBy(F.col("currency_max_tier_value_rank").desc_nulls_first())
w12 = Window.orderBy(F.col("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

with_windows = (
    flattened
    .select(
        "id",
        "plan",
        "effective_from",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.max("tier_value").over(w2).alias("currency_max_tier_value"),
    )
)

enriched = (
    with_windows
    .withColumn("effective_from_year_stddev_samp_currency_max_tier_value", F.stddev_samp("currency_max_tier_value").over(w3))
    .withColumn("tier_value_cume_dist_cume_dist", F.cume_dist().over(w4))
    .withColumn("plan_count_tier_value_cume_dist", F.count("tier_value_cume_dist").over(w5))
    .withColumn("tier_value_cume_dist_cume_dist_2", F.cume_dist().over(w_ordered))
)

scored = (
    enriched
    .filter(F.col("effective_from_year_stddev_samp_currency_max_tier_value").between(32, 68))
    .select(
        "tier_value_cume_dist",
        "plan_count_tier_value_cume_dist",
        "tier_value_cume_dist_cume_dist_2",
        F.cume_dist().over(w7).alias("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist"),
        F.rank().over(w8).alias("currency_max_tier_value_rank"),
    )
)

cleaned = (
    scored
    .filter(F.col("tier_value_cume_dist_cume_dist_2").between(0.23, 0.26))
    .select(
        "plan_count_tier_value_cume_dist",
        "tier_value_cume_dist_cume_dist_2",
        "effective_from_year_stddev_samp_currency_max_tier_value_cume_dist",
        "currency_max_tier_value_rank",
        F.round((F.col("currency_max_tier_value_rank") * 0.9), 2).alias("currency_max_tier_value_rank_adjusted"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w9).alias("currency_max_tier_value_rank_adjusted_cume_dist"),
        F.dense_rank().over(w_part).alias("currency_max_tier_value_rank_adjusted_dense_rank"),
        F.cume_dist().over(w11).alias("currency_max_tier_value_rank_cume_dist"),
        F.percent_rank().over(w12).alias("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_percent_rank"),
    )
)

cleaned_price_tiers = (
    metrics
    .filter(F.col("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_percent_rank") >= 0.3)
    .select(
        "plan_count_tier_value_cume_dist",
        "tier_value_cume_dist_cume_dist_2",
        "effective_from_year_stddev_samp_currency_max_tier_value_cume_dist",
        "currency_max_tier_value_rank_adjusted",
        "currency_max_tier_value_rank_adjusted_cume_dist",
        "currency_max_tier_value_rank_adjusted_dense_rank",
        "effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_percent_rank",
        F.least("currency_max_tier_value_rank_adjusted_dense_rank", "currency_max_tier_value_rank").alias("least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank"),
    )
)

result = (
    cleaned_price_tiers
    .withColumn("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_tier", F.when(F.col("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist") >= 0.81, "HIGH").otherwise("LOW"))
    .withColumn("plan_count_tier_value_cume_dist_tier", F.when(F.col("plan_count_tier_value_cume_dist") >= 18, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_tier", "plan_count_tier_value_cume_dist_tier"], ["effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_tier"]],
        "effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_tier",
        "plan_count_tier_value_cume_dist_tier",
    )
    .agg(
        F.percentile_approx("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist", 0.5).alias("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_p50"),
        F.round(F.avg("least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank"), 2).alias("avg_least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank"),
        F.percentile_approx("tier_value_cume_dist_cume_dist_2", 0.5).alias("tier_value_cume_dist_cume_dist_2_p50"),
        F.percentile_approx("least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank", [0.25, 0.5, 0.75]).alias("least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank_quartiles"),
        F.percentile_approx("currency_max_tier_value_rank_adjusted", [0.25, 0.5, 0.75]).alias("currency_max_tier_value_rank_adjusted_quartiles"),
        F.percentile_approx("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist", [0.25, 0.5, 0.75]).alias("effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_quartiles"),
    )
    .orderBy(
        F.col("avg_least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank").desc(),
    )
)
