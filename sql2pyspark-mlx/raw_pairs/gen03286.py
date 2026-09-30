from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "effective_from"), "currency").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("val").asc_nulls_first())
w_running = Window.orderBy(F.col("val").asc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w5 = Window.partitionBy("currency", "plan").orderBy(F.col("val_cume_dist").desc_nulls_first())
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("currency")
w8 = Window.partitionBy("plan").orderBy(F.col("plan_count_val_filled").desc_nulls_last())
w9 = Window.orderBy(F.col("plan_count_val_filled").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

valid = (
    flattened
    .filter(F.col("effective_from") <= "2024-04-29")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        F.coalesce("val", F.lit(0)).alias("val_filled"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val_filled",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
        F.rank().over(w_running).alias("val_rank"),
        F.percent_rank().over(w4).alias("val_percent_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("val_dense_rank").between(3, 5))
    .select(
        "plan",
        "currency",
        "val_filled",
        "val_cume_dist",
        "val_dense_rank",
        F.round((F.col("val_dense_rank") * 1.15), 2).alias("adjusted_val_dense_rank"),
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

enriched = (
    cleaned
    .select(
        "plan",
        "currency",
        "adjusted_val_dense_rank",
        F.cume_dist().over(w5).alias("val_cume_dist_cume_dist"),
        F.count("val_dense_rank").over(w6).alias("plan_count_val_dense_rank"),
        F.count("val_filled").over(w6).alias("plan_count_val_filled"),
    )
)

scored = (
    enriched
    .filter(F.col("adjusted_val_dense_rank") <= 30)
    .select(
        "*",
        F.avg("adjusted_val_dense_rank").over(w6).alias("plan_avg_adjusted_val_dense_rank"),
        F.min("adjusted_val_dense_rank").over(w6).alias("plan_min_adjusted_val_dense_rank"),
        F.round((F.col("plan_count_val_dense_rank") / F.sum("plan_count_val_dense_rank").over(w7)), 4).alias("plan_count_val_dense_rank_share"),
        F.percent_rank().over(w8).alias("plan_count_val_filled_percent_rank"),
        F.cume_dist().over(w9).alias("plan_count_val_filled_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.col("plan_count_val_filled") < 7)
    .select(
        "*",
        F.greatest("plan_count_val_filled_percent_rank", "plan_count_val_filled_cume_dist").alias("greatest_plan_count_val_filled_percent_rank_plan_count_val_filled_cume_dist"),
        (F.col("plan_count_val_filled_cume_dist") / F.nullif("val_cume_dist_cume_dist", F.lit(0))).alias("plan_count_val_filled_cume_dist_per_val_cume_dist_cume_dist"),
    )
)

stats = (
    filtered
    .groupBy("plan", "currency")
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.max(F.when(F.col("plan") == "basic", F.col("plan_count_val_dense_rank_share"))).alias("basic_max_plan_count_val_dense_rank_share"),
        F.percentile_approx("plan_count_val_filled_percent_rank", [0.25, 0.5, 0.75]).alias("plan_count_val_filled_percent_rank_quartiles"),
    )
)

result = (
    stats
    .select(
        "plan",
        "currency",
        "usd_count",
        "enterprise_events",
        "plan_list",
        "basic_max_plan_count_val_dense_rank_share",
        "plan_count_val_filled_percent_rank_quartiles",
    )
    .orderBy(F.col("plan").desc(), F.col("currency").desc())
)
