from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("currency_count_val").desc())
w4 = Window.orderBy(F.col("currency_count_val").desc_nulls_first())
w5 = Window.orderBy("currency_count_val")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

matched_keys = (
    lines
    .select("currency")
    .intersect(
        df_price_tiers
        .filter(F.col("currency") != "USD")
        .select("currency")
    )
)

verified = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "val",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.count("val").over(w2).alias("currency_count_val"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "val",
        "currency_count_val",
        F.rank().over(w3).alias("currency_count_val_rank"),
        F.percent_rank().over(w3).alias("currency_count_val_percent_rank"),
        F.dense_rank().over(w4).alias("currency_count_val_dense_rank"),
        F.cume_dist().over(w5).alias("currency_count_val_cume_dist"),
    )
)

result = (
    metrics
    .rollup(
        F.when(F.col("currency_count_val_cume_dist") >= 0.64, "LARGE").otherwise("SMALL").alias("currency_count_val_cume_dist_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("currency_count_val_percent_rank", 0.9).alias("currency_count_val_percent_rank_p90"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("val", 0.95).alias("val_p95"),
        F.min("currency_count_val_dense_rank").alias("min_currency_count_val_dense_rank"),
    )
    .orderBy("currency_count_val_cume_dist_tier")
)
