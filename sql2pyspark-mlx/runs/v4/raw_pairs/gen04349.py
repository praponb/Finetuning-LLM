from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("plan", "currency")
w6 = Window.partitionBy("currency", "plan").orderBy(F.col("currency_sum_tier_value").asc_nulls_last())
w7 = Window.partitionBy("plan").orderBy(F.col("plan_max_currency_sum_tier_value").desc())
w8 = Window.orderBy(F.col("currency_sum_tier_value").desc_nulls_last())
w9 = Window.partitionBy("currency", "plan")
w10 = Window.partitionBy("plan").orderBy(F.col("plan_max_currency_sum_tier_value").asc_nulls_first())
w11 = Window.partitionBy("currency").orderBy(F.col("currency_max_plan_max_currency_sum_tier_value").desc())
w12 = Window.partitionBy("currency").orderBy(F.col("plan_max_currency_sum_tier_value_rank").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

metrics = (
    exploded
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("currency_sum_tier_value", F.sum("tier_value").over(w2))
    .withColumn("tier_value_rank", F.rank().over(w3))
    .withColumn("plan_avg_tier_value", F.avg("tier_value").over(w4))
)

enriched = (
    metrics
    .withColumn("currency_min_tier_value_cume_dist", F.min("tier_value_cume_dist").over(w2))
    .withColumn("plan_max_currency_sum_tier_value", F.max("currency_sum_tier_value").over(w5))
    .withColumn("currency_sum_tier_value_rank", F.rank().over(w6))
)

with_windows = (
    enriched
    .select(
        "*",
        F.rank().over(w7).alias("plan_max_currency_sum_tier_value_rank"),
        F.cume_dist().over(w8).alias("currency_sum_tier_value_cume_dist"),
        F.max("plan_max_currency_sum_tier_value").over(w9).alias("currency_max_plan_max_currency_sum_tier_value"),
        F.rank().over(w10).alias("plan_max_currency_sum_tier_value_rank_2"),
        F.sum("plan_avg_tier_value").over(w2).alias("currency_sum_plan_avg_tier_value"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.max("currency_sum_tier_value_cume_dist").over(w2).alias("currency_max_currency_sum_tier_value_cume_dist"),
        F.avg("tier_value_cume_dist").over(w4).alias("plan_avg_tier_value_cume_dist"),
        F.rank().over(w11).alias("currency_max_plan_max_currency_sum_tier_value_rank"),
        F.sum("currency_sum_tier_value").over(w2).alias("currency_sum_currency_sum_tier_value"),
        F.dense_rank().over(w12).alias("plan_max_currency_sum_tier_value_rank_dense_rank"),
    )
)

valid = (
    with_windows_price_tiers
    .filter(~F.col("currency").isin(["EUR", "USD", "THB"]))
    .withColumn("currency_sum_tier_value_rounded", F.round("currency_sum_tier_value", 1))
    .withColumn("plan_max_currency_sum_tier_value_rank_num", F.col("plan_max_currency_sum_tier_value_rank").cast("double"))
)

metrics_price_tiers = (
    valid
    .withColumn("plan_stddev_samp_plan_avg_tier_value_cume_dist", F.stddev_samp("plan_avg_tier_value_cume_dist").over(w4))
    .withColumn("currency_sum_plan_avg_tier_value_cume_dist", F.sum("plan_avg_tier_value_cume_dist").over(w2))
)

stats = (
    metrics_price_tiers
    .cube(
        "currency",
        F.when(F.col("currency_sum_plan_avg_tier_value") >= 324, "LARGE").otherwise("SMALL").alias("currency_sum_plan_avg_tier_value_tier"),
        "plan",
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("plan_avg_tier_value"))).alias("eur_total_plan_avg_tier_value"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.round(F.sum("currency_max_plan_max_currency_sum_tier_value_rank") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("currency_max_plan_max_currency_sum_tier_value_rank_per_enterprise"),
    )
)

result = (
    stats
    .select("currency_sum_plan_avg_tier_value_tier", "plan", "usd_count")
    .orderBy("plan")
)
