from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy(F.date_trunc("WEEK", "effective_from"))
w4 = Window.partitionBy("plan").orderBy(F.col("effective_from_period_stddev_samp_tier_value").asc_nulls_last())
w5 = Window.partitionBy("plan").orderBy(F.col("effective_from_period_stddev_samp_tier_value").desc_nulls_first())
w6 = Window.partitionBy("plan").orderBy(F.col("tier_value_dense_rank").desc())
w7 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("effective_from_period_stddev_samp_tier_value").asc_nulls_last())

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

base = (
    flattened
    .filter(F.year("effective_from") == 2024)
    .select(
        "plan",
        "effective_from",
        "tier_value",
        F.concat("plan", F.lit(" "), "currency").alias("plan_currency"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("tier_value") < 44)
    .withColumn("plan_length", F.length("plan"))
    .withColumn("effective_from_period", F.date_trunc("month", "effective_from"))
)

with_windows = (
    base_price_tiers
    .select(
        "plan",
        "effective_from",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.min("plan_length").over(w2).alias("plan_min_plan_length"),
        F.stddev_samp("tier_value").over(w3).alias("effective_from_period_stddev_samp_tier_value"),
    )
)

with_windows_price_tiers = (
    with_windows
    .filter(F.col("plan") == "enterprise")
    .select(
        "plan",
        "effective_from",
        "plan_min_plan_length",
        F.percent_rank().over(w4).alias("effective_from_period_stddev_samp_tier_value_percent_rank"),
        F.percent_rank().over(w5).alias("effective_from_period_stddev_samp_tier_value_percent_rank_2"),
        F.percent_rank().over(w6).alias("tier_value_dense_rank_percent_rank"),
        F.percent_rank().over(w7).alias("effective_from_period_stddev_samp_tier_value_percent_rank_3"),
    )
)

grouped = (
    with_windows_price_tiers
    .groupBy(
        F.when(F.col("plan_min_plan_length") >= 61, "PREMIUM").otherwise("STANDARD").alias("plan_min_plan_length_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("plan") == "basic", F.col("effective_from_period_stddev_samp_tier_value_percent_rank_2"))).alias("basic_avg_effective_from_period_stddev_samp_tier_value_percent_rank_2"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("plan") == "pro", F.col("effective_from_period_stddev_samp_tier_value_percent_rank_2")).otherwise(0)).alias("pro_effective_from_period_stddev_samp_tier_value_percent_rank_2"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .filter(F.col("pro_effective_from_period_stddev_samp_tier_value_percent_rank_2") > 0)
)

result = (
    grouped
    .select(
        "plan_min_plan_length_tier",
        "num_rows",
        "basic_avg_effective_from_period_stddev_samp_tier_value_percent_rank_2",
        "event_count",
        "pro_effective_from_period_stddev_samp_tier_value_percent_rank_2",
        "enterprise_count",
    )
    .orderBy(
        F.col("num_rows").desc(),
        "pro_effective_from_period_stddev_samp_tier_value_percent_rank_2",
    )
)
