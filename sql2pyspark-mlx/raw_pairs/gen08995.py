from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy(F.month("effective_from"), "currency").orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy(F.year("effective_from"), "currency").orderBy(F.col("val_cume_dist").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("val_cume_dist").desc())

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

scored = exploded.withColumn("currency_max_val", F.max("val").over(w))

scored_price_tiers = scored.select("*", F.cume_dist().over(w2).alias("val_cume_dist"))

enriched = (
    scored_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "val_cume_dist",
        F.rank().over(w3).alias("val_cume_dist_rank"),
    )
)

base = (
    enriched
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-05-16"))) & ~F.col("currency").isin(["USD", "THB"]))
    .withColumn("val_cume_dist_int", F.col("val_cume_dist").cast("int"))
    .withColumn("adjusted_val", F.round((F.col("val") * 100), 2))
)

with_windows = (
    base
    .withColumn("currency_sum_val_cume_dist_int", F.sum("val_cume_dist_int").over(w))
    .withColumn("val_cume_dist_dense_rank", F.dense_rank().over(w4))
)

cleaned = (
    with_windows
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-01-23"))) & (F.col("val_cume_dist") > 0.26))
    .select(
        "effective_from",
        "val_cume_dist_rank",
        "val_cume_dist_int",
        "adjusted_val",
        "currency_sum_val_cume_dist_int",
        "val_cume_dist_dense_rank",
        (F.col("val") + F.col("val_cume_dist_rank")).alias("val_plus_val_cume_dist_rank"),
    )
)

summary = (
    cleaned
    .groupBy(
        F.when(F.col("val_cume_dist_int") >= 85, "PREMIUM").otherwise("STANDARD").alias("val_cume_dist_int_tier"),
    )
    .agg(
        F.percentile_approx("val_cume_dist_dense_rank", [0.25, 0.5, 0.75]).alias("val_cume_dist_dense_rank_quartiles"),
        F.percentile_approx("currency_sum_val_cume_dist_int", 0.9).alias("currency_sum_val_cume_dist_int_p90"),
        F.percentile_approx("val_cume_dist_rank", 0.5).alias("val_cume_dist_rank_p50"),
        F.percentile_approx("currency_sum_val_cume_dist_int", 0.9).alias("currency_sum_val_cume_dist_int_p90_2"),
        F.round(F.avg("currency_sum_val_cume_dist_int"), 2).alias("avg_currency_sum_val_cume_dist_int"),
    )
)

result = (
    summary
    .select(
        "val_cume_dist_int_tier",
        "val_cume_dist_dense_rank_quartiles",
        "currency_sum_val_cume_dist_int_p90",
        "val_cume_dist_rank_p50",
        "currency_sum_val_cume_dist_int_p90_2",
        "avg_currency_sum_val_cume_dist_int",
    )
    .orderBy("val_cume_dist_rank_p50")
)
