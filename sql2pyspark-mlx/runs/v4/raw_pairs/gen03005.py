from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("currency")
w_running = Window.partitionBy("plan")
w_recent = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w5 = Window.orderBy(F.col("val").asc_nulls_first())
w6 = Window.partitionBy("currency", "plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.min("val").over(w).alias("plan_min_val"),
        F.avg("val").over(w2).alias("currency_avg_val"),
    )
)

filtered = (
    metrics
    .filter(F.col("plan_min_val") < 52)
    .withColumn("plan_currency", F.concat("plan", F.lit(" - "), "currency"))
    .withColumn("plan_currency_key", F.concat_ws(",", "plan", "currency"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.max("val").over(w_running).alias("plan_max_val"),
        F.max("currency_avg_val").over(w_running).alias("plan_max_currency_avg_val"),
    )
)

remaining_keys = (
    with_windows
    .select("plan", "id")
    .intersect(
        df_price_tiers
        .filter(F.col("effective_from") >= "2025-09-29")
        .select("plan", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("val_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w5))
)

enriched = (
    scored
    .select(
        "*",
        F.stddev_samp("val_percent_rank").over(w_running).alias("plan_stddev_samp_val_percent_rank"),
        F.max("plan_max_val").over(w6).alias("currency_max_plan_max_val"),
        F.count("val_percent_rank").over(w).alias("plan_count_val_percent_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "plan",
        "currency",
        "val",
        "currency_avg_val",
        "plan_currency",
        "plan_max_currency_avg_val",
        "val_percent_rank",
        "val_percent_rank_2",
    )
)
