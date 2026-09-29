from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("is_usd").asc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("is_usd").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy("is_usd")
w5 = Window.partitionBy("currency").orderBy(F.col("is_usd_cume_dist_3").asc_nulls_last())
w6 = Window.partitionBy("is_usd_cume_dist_3_rank_tier").orderBy(F.col("currency_stddev_samp_tier_value_p90").desc())

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("tier_value"))
)

valid = (
    exploded
    .filter(F.col("tier_value") > 62)
    .select("*", F.when(F.col("currency") == "USD", 1).otherwise(0).alias("is_usd"))
)

scored = (
    valid
    .withColumn("is_usd_cume_dist", F.cume_dist().over(w))
    .withColumn("currency_max_is_usd", F.max("is_usd").over(w2))
    .withColumn("is_usd_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("currency_sum_is_usd", F.sum("is_usd").over(w2))
    .withColumn("is_usd_cume_dist_3", F.cume_dist().over(w4))
)

enriched = (
    scored
    .filter(F.col("currency_sum_is_usd") >= 260)
    .select(
        "is_usd",
        "currency_max_is_usd",
        "is_usd_cume_dist_2",
        F.sum("is_usd_cume_dist_3").over(w2).alias("currency_sum_is_usd_cume_dist_3"),
        F.stddev_samp("tier_value").over(w2).alias("currency_stddev_samp_tier_value"),
        F.rank().over(w5).alias("is_usd_cume_dist_3_rank"),
        F.stddev_samp("tier_value").over(w2).alias("currency_stddev_samp_tier_value_2"),
    )
)

base = (
    enriched
    .filter(F.col("is_usd_cume_dist_2") >= 0.71)
    .select(
        "*",
        F.round("currency_stddev_samp_tier_value_2", 0).alias("currency_stddev_samp_tier_value_2_rounded"),
    )
)

grouped = (
    base
    .groupBy(
        F.when(F.col("is_usd_cume_dist_3_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("is_usd_cume_dist_3_rank_tier"),
        F.when(F.col("is_usd") >= 81, "LARGE").otherwise("SMALL").alias("is_usd_tier"),
    )
    .agg(
        F.round(F.avg("is_usd"), 2).alias("avg_is_usd"),
        F.percentile_approx("currency_max_is_usd", [0.25, 0.5, 0.75]).alias("currency_max_is_usd_quartiles"),
        F.percentile_approx("currency_stddev_samp_tier_value", 0.9).alias("currency_stddev_samp_tier_value_p90"),
    )
    .filter(F.col("avg_is_usd") >= 0)
    .withColumn("currency_stddev_samp_tier_value_p90_percent_rank", F.percent_rank().over(w6))
)

result = (
    grouped
    .select(
        "is_usd_cume_dist_3_rank_tier",
        "is_usd_tier",
        "currency_max_is_usd_quartiles",
        "currency_stddev_samp_tier_value_p90",
    )
    .orderBy(F.col("is_usd_tier").desc_nulls_first())
)
