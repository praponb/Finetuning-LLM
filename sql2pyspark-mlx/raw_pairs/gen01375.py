from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("val").desc())
w4 = Window.partitionBy(F.month("effective_from"), "plan")
w5 = Window.partitionBy("plan", "currency")
w6 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())
w7 = Window.partitionBy("plan", "currency").orderBy(F.col("plan_min_val").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

with_windows = (
    exploded
    .filter(F.col("effective_from") <= "2025-05-01")
    .select(
        "*",
        F.min("val").over(w).alias("plan_min_val"),
        F.rank().over(w2).alias("val_rank"),
        F.rank().over(w3).alias("val_rank_2"),
    )
)

metrics = (
    with_windows
    .withColumn("effective_from_month_stddev_samp_val_rank_2", F.stddev_samp("val_rank_2").over(w4))
    .withColumn("plan_min_val_2", F.min("val").over(w5))
    .withColumn("val_percent_rank", F.percent_rank().over(w6))
    .withColumn("plan_min_val_rank", F.rank().over(w7))
)

valid = (
    metrics
    .filter(F.col("plan_min_val") == 33)
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        "plan_min_val",
        "val_rank",
        "val_rank_2",
        "effective_from_month_stddev_samp_val_rank_2",
        "plan_min_val_2",
        "val_percent_rank",
        "plan_min_val_rank",
        F.add_months("effective_from", 12).alias("effective_from_plus_months"),
    )
)

result = (
    valid
    .rollup(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("plan_min_val_rank", 0.5).alias("plan_min_val_rank_p50"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.max(F.when(F.col("plan") == "basic", F.col("plan_min_val"))).alias("basic_max_plan_min_val"),
    )
    .orderBy(F.col("distinct_plans").desc())
)
