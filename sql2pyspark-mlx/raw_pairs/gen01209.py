from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value_cume_dist_filled").asc_nulls_first())
w3 = Window.partitionBy(F.date_trunc("YEAR", "effective_from"))
w4 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_first())
w5 = Window.partitionBy(F.year("effective_from")).orderBy("tier_value_cume_dist_filled")
w6 = Window.orderBy("tier_value_cume_dist")

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("tier_value"))
)

metrics = (
    exploded
    .filter(F.col("tier_value").between(36, 64))
    .select("id", "effective_from", F.cume_dist().over(w).alias("tier_value_cume_dist"))
)

base = (
    metrics
    .filter(F.month("effective_from").isin([4, 5, 8]))
    .withColumn("tier_value_cume_dist_filled", F.coalesce("tier_value_cume_dist", F.lit(0)))
)

scored = (
    base
    .select(
        "effective_from",
        "tier_value_cume_dist",
        F.cume_dist().over(w2).alias("tier_value_cume_dist_filled_cume_dist"),
        F.stddev_samp("tier_value_cume_dist").over(w3).alias("effective_from_period_stddev_samp_tier_value_cume_dist"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist_cume_dist"),
        F.rank().over(w5).alias("tier_value_cume_dist_filled_rank"),
        F.rank().over(w6).alias("tier_value_cume_dist_rank"),
    )
)

result = (
    scored
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .rollup("activity_month")
    .agg(
        F.round(F.avg("tier_value_cume_dist"), 2).alias("avg_tier_value_cume_dist"),
        F.min("tier_value_cume_dist").alias("min_tier_value_cume_dist"),
        F.min("tier_value_cume_dist_rank").alias("min_tier_value_cume_dist_rank"),
        F.percentile_approx("tier_value_cume_dist", 0.95).alias("tier_value_cume_dist_p95"),
        F.grouping_id().alias("grouping_level"),
    )
)
