from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency")
w2 = Window.partitionBy(F.month("effective_from"))
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy("currency_max_val")
w5 = Window.orderBy(F.col("currency_stddev_samp_currency_max_val").desc_nulls_last())
w6 = Window.partitionBy("currency").orderBy(F.col("currency_sum_val").desc())
w7 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

metrics = (
    lines
    .withColumn("currency_sum_val", F.sum("val").over(w_hist))
    .withColumn("currency_max_val", F.max("val").over(w_hist))
)

kept_keys = (
    metrics
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "enterprise")
        .select("currency", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_price_tiers = (
    reconciled
    .select(
        "id",
        "effective_from",
        "currency",
        "currency_sum_val",
        "currency_max_val",
        F.sum("currency_max_val").over(w2).alias("effective_from_month_sum_currency_max_val"),
        F.percent_rank().over(w3).alias("val_percent_rank"),
        F.stddev_samp("currency_max_val").over(w_hist).alias("currency_stddev_samp_currency_max_val"),
        F.rank().over(w4).alias("currency_max_val_rank"),
        F.stddev_samp("currency_sum_val").over(w_hist).alias("currency_stddev_samp_currency_sum_val"),
    )
)

valid = (
    metrics_price_tiers
    .filter(F.col("val_percent_rank") <= 0.3)
    .withColumn("currency_stddev_samp_currency_sum_val_rounded", F.round("currency_stddev_samp_currency_sum_val", 2))
)

with_windows = (
    valid
    .filter(F.col("currency_stddev_samp_currency_sum_val_rounded") <= 55)
    .withColumn("currency_stddev_samp_currency_max_val_cume_dist", F.cume_dist().over(w5))
    .withColumn("currency_sum_val_cume_dist", F.cume_dist().over(w6))
    .withColumn("val_percent_rank_percent_rank", F.percent_rank().over(w7))
)

filtered = (
    with_windows
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-02-24")))
    .select(
        "id",
        "effective_from",
        "currency",
        "currency_sum_val",
        "currency_max_val",
        "effective_from_month_sum_currency_max_val",
        "val_percent_rank",
        "currency_stddev_samp_currency_max_val",
        "currency_max_val_rank",
        "currency_stddev_samp_currency_sum_val_rounded",
        "currency_stddev_samp_currency_max_val_cume_dist",
        "val_percent_rank_percent_rank",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

summary = (
    filtered
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.percentile_approx("val_percent_rank_percent_rank", 0.5).alias("val_percent_rank_percent_rank_p50"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("currency_max_val", 0.5).alias("currency_max_val_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("val_percent_rank_percent_rank_p50") > 0)
)

result = summary.select("week_start", "usd_events", "currency_max_val_p50", "distinct_ids")
