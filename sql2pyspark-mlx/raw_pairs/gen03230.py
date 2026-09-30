from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("effective_from"), "plan")
w2 = Window.orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy(F.date_trunc("WEEK", "effective_from"), "plan")
w4 = Window.orderBy(F.col("val").desc())
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("plan").orderBy(F.col("effective_from_month_avg_val").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

with_windows = (
    exploded
    .filter(F.col("effective_from") > "2025-08-04")
    .select(
        "id",
        "plan",
        F.avg("val").over(w).alias("effective_from_month_avg_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.stddev_samp("val").over(w3).alias("effective_from_period_stddev_samp_val"),
        F.cume_dist().over(w4).alias("val_cume_dist"),
        F.stddev_samp("val").over(w5).alias("plan_stddev_samp_val"),
    )
)

base = (
    with_windows
    .filter(F.col("effective_from_month_avg_val").between(40, 60))
    .select(
        "id",
        "plan",
        "effective_from_month_avg_val",
        "val_percent_rank",
        "effective_from_period_stddev_samp_val",
        "val_cume_dist",
        F.upper("plan").alias("plan_upper"),
    )
)

filtered = (
    base
    .filter(F.col("val_cume_dist") >= 0.45)
    .withColumn("adjusted_val_percent_rank", F.round((F.col("val_percent_rank") * 1.07), 2))
)

metrics = (
    filtered
    .select(
        "val_percent_rank",
        "effective_from_period_stddev_samp_val",
        "val_cume_dist",
        "adjusted_val_percent_rank",
        F.sum("effective_from_period_stddev_samp_val").over(w5).alias("plan_sum_effective_from_period_stddev_samp_val"),
        F.cume_dist().over(w6).alias("effective_from_month_avg_val_cume_dist"),
    )
)

base_price_tiers = (
    metrics
    .filter(F.col("effective_from_period_stddev_samp_val") >= 23)
    .withColumn("plan_sum_effective_from_period_stddev_samp_val_adjusted", F.round((F.col("plan_sum_effective_from_period_stddev_samp_val") * 0.9), 2))
)

result = (
    base_price_tiers
    .select(
        "effective_from_period_stddev_samp_val",
        "val_cume_dist",
        "effective_from_month_avg_val_cume_dist",
        "plan_sum_effective_from_period_stddev_samp_val_adjusted",
    )
    .orderBy(
        F.col("plan_sum_effective_from_period_stddev_samp_val_adjusted").desc_nulls_last(),
    )
)
