from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val_filled").desc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy(F.date_trunc("MONTH", "effective_from"))
w4 = Window.partitionBy("currency").orderBy(F.col("currency_count_val_filled_per_currency_avg_val").desc_nulls_first())
w5 = Window.orderBy(F.col("currency_min_val_filled").asc_nulls_last())
w_part = Window.partitionBy("currency").orderBy(F.col("val_filled").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

cleaned = (
    lines
    .filter(F.col("effective_from") >= "2024-05-01")
    .withColumn("effective_from_label", F.date_format("effective_from", "MMM yyyy"))
    .withColumn("currency_prefix", F.substring("currency", 1, 3))
)

base = (
    cleaned
    .filter(F.col("effective_from") >= F.lit("2024-02-28").cast("date"))
    .select(
        "effective_from",
        "currency",
        "val",
        "effective_from_label",
        "currency_prefix",
        F.coalesce("val", F.lit(0)).alias("val_filled"),
    )
)

enriched = (
    base
    .withColumn("val_filled_cume_dist", F.cume_dist().over(w))
    .withColumn("currency_min_val_filled", F.min("val_filled").over(w2))
    .withColumn("currency_count_val_filled", F.count("val_filled").over(w2))
)

enriched_price_tiers = enriched.withColumn("currency_avg_val", F.avg("val").over(w2))

filtered = (
    enriched_price_tiers
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-06-18"))) & (F.col("val_filled") > 46))
    .select(
        "*",
        (F.col("currency_count_val_filled") / F.nullif("currency_avg_val", F.lit(0))).alias("currency_count_val_filled_per_currency_avg_val"),
    )
)

metrics = (
    filtered
    .select(
        "effective_from",
        "val",
        "currency_prefix",
        "val_filled",
        "val_filled_cume_dist",
        "currency_min_val_filled",
        F.sum("val_filled").over(w3).alias("effective_from_period_sum_val_filled"),
        F.avg("val").over(w2).alias("currency_avg_val_2"),
        F.rank().over(w4).alias("currency_count_val_filled_per_currency_avg_val_rank"),
        F.cume_dist().over(w5).alias("currency_min_val_filled_cume_dist"),
        F.rank().over(w_part).alias("val_filled_rank"),
    )
)

result = (
    metrics
    .rollup(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.when(F.col("val_filled_cume_dist") >= 0.79, "HIGH").otherwise("LOW").alias("val_filled_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("currency_min_val_filled_cume_dist", [0.25, 0.5, 0.75]).alias("currency_min_val_filled_cume_dist_quartiles"),
        F.round(F.avg("effective_from_period_sum_val_filled"), 2).alias("avg_effective_from_period_sum_val_filled"),
        F.round(F.avg("currency_avg_val_2"), 2).alias("avg_currency_avg_val_2"),
        F.percentile_approx("currency_min_val_filled", [0.25, 0.5, 0.75]).alias("currency_min_val_filled_quartiles"),
        F.count("*").alias("event_count"),
    )
)
