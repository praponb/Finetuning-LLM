from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "effective_from"))
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w4 = Window.partitionBy("currency")
w5 = Window.partitionBy(F.date_trunc("YEAR", "effective_from"), "currency").orderBy(F.col("val").asc_nulls_first())
w6 = Window.orderBy(F.col("val_plus_effective_from_period_sum_val").asc_nulls_last())
w7 = Window.orderBy(F.col("val_rank").desc_nulls_first())
w_running = Window.orderBy(F.col("val_plus_effective_from_period_sum_val_cume_dist").asc_nulls_first())
w9 = Window.orderBy(F.col("val_plus_effective_from_period_sum_val").asc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

metrics = (
    exploded
    .select(
        "plan",
        "effective_from",
        "currency",
        "val",
        F.sum("val").over(w).alias("effective_from_period_sum_val"),
        F.count("val").over(w2).alias("plan_count_val"),
        F.cume_dist().over(w3).alias("val_cume_dist"),
        F.count("val").over(w4).alias("currency_count_val"),
        F.rank().over(w5).alias("val_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-04-20").cast("date"))
    .select(
        "effective_from",
        "val",
        "effective_from_period_sum_val",
        "plan_count_val",
        "val_rank",
        F.initcap(F.trim("currency")).alias("currency_title"),
    )
)

base = (
    cleaned
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-05-28"))) & (F.col("val") >= 49))
    .select(
        "effective_from",
        "effective_from_period_sum_val",
        "val_rank",
        "currency_title",
        F.date_add("effective_from", 57).alias("effective_from_plus_57d"),
        (F.col("val") + F.col("effective_from_period_sum_val")).alias("val_plus_effective_from_period_sum_val"),
    )
)

with_windows = (
    base
    .withColumn("val_plus_effective_from_period_sum_val_cume_dist", F.cume_dist().over(w6))
    .withColumn("val_rank_cume_dist", F.cume_dist().over(w7))
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w_running).alias("val_plus_effective_from_period_sum_val_cume_dist_cume_dist"),
        F.dense_rank().over(w9).alias("val_plus_effective_from_period_sum_val_dense_rank"),
    )
)

base_price_tiers = (
    with_windows_price_tiers
    .filter(F.col("val_rank") == 4)
    .select(
        "*",
        (F.datediff(F.current_date(), "effective_from") / 365.25).alias("years_since_effective_from"),
    )
)

result = (
    base_price_tiers
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.avg("val_rank").alias("avg_val_rank"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("val_plus_effective_from_period_sum_val", [0.25, 0.5, 0.75]).alias("val_plus_effective_from_period_sum_val_quartiles"),
        F.max("val_rank_cume_dist").alias("max_val_rank_cume_dist"),
        F.percentile_approx("val_rank_cume_dist", 0.9).alias("val_rank_cume_dist_p90"),
    )
    .orderBy(F.col("val_rank_cume_dist_p90").desc())
)
