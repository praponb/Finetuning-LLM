from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("starts_on"), "promo_code").orderBy("discount_pct")
w2 = Window.partitionBy("channel")
w_part = Window.orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy(F.date_trunc("month", "starts_on"), "channel").orderBy(F.col("discount_pct_share").asc_nulls_last())
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_dense_rank").desc_nulls_first())
w6 = Window.partitionBy("promo_code", "channel")
w7 = Window.orderBy("discount_pct")
w8 = Window.orderBy(F.col("channel_count_discount_pct").desc())
w9 = Window.orderBy(F.col("discount_pct_dense_rank").asc_nulls_last())
w10 = Window.orderBy("channel_count_discount_pct")
w11 = Window.orderBy(F.col("promo_code_count_discount_pct").asc_nulls_first())
w12 = Window.orderBy(F.col("channel_count_discount_pct_dense_rank_2").asc_nulls_last())
w13 = Window.orderBy(F.col("discount_pct_dense_rank_2").desc_nulls_last())
w14 = Window.orderBy(F.col("discount_pct_dense_rank_rank").desc_nulls_first())

remaining_keys = (
    df_promotions
    .select("channel", "promo_code")
    .intersect(
        df_promotions
        .filter(F.col("channel").isin(["partner", "mobile"]))
        .select("channel", "promo_code")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("ends_on").between("2023-09-01", "2024-12-01"))
    .select(
        "*",
        F.cume_dist().over(w).alias("discount_pct_cume_dist"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.count("discount_pct").over(w2).alias("channel_count_discount_pct"),
        F.dense_rank().over(w_part).alias("discount_pct_dense_rank"),
    )
)

with_windows_promotions = (
    with_windows
    .select(
        "id",
        "discount_pct_cume_dist",
        "discount_pct_share",
        "channel_count_discount_pct",
        "discount_pct_dense_rank",
        F.dense_rank().over(w4).alias("discount_pct_share_dense_rank"),
        F.dense_rank().over(w5).alias("discount_pct_dense_rank_dense_rank"),
        F.count("discount_pct").over(w6).alias("promo_code_count_discount_pct"),
        F.dense_rank().over(w7).alias("discount_pct_dense_rank_2"),
        F.rank().over(w_part).alias("discount_pct_rank"),
    )
)

metrics = (
    with_windows_promotions
    .select(
        "id",
        "discount_pct_share",
        "discount_pct_share_dense_rank",
        "promo_code_count_discount_pct",
        "discount_pct_dense_rank_2",
        F.dense_rank().over(w8).alias("channel_count_discount_pct_dense_rank"),
        F.rank().over(w9).alias("discount_pct_dense_rank_rank"),
        F.dense_rank().over(w10).alias("channel_count_discount_pct_dense_rank_2"),
    )
)

filtered = (
    metrics
    .filter(F.col("discount_pct_share") > 0.61)
    .withColumn("channel_count_discount_pct_dense_rank_2_adjusted", F.round((F.col("channel_count_discount_pct_dense_rank_2") * 1.15), 2))
    .withColumn("discount_pct_share_dense_rank_band", F.when(F.col("discount_pct_share_dense_rank") >= 8, "High").when(F.col("discount_pct_share_dense_rank") >= 3, "Medium").otherwise("Low"))
)

with_windows_promotions_2 = (
    filtered
    .select(
        "discount_pct_share",
        "promo_code_count_discount_pct",
        "discount_pct_dense_rank_2",
        F.rank().over(w11).alias("promo_code_count_discount_pct_rank"),
        F.dense_rank().over(w12).alias("channel_count_discount_pct_dense_rank_2_dense_rank"),
        F.percent_rank().over(w13).alias("discount_pct_dense_rank_2_percent_rank"),
        F.percent_rank().over(w14).alias("discount_pct_dense_rank_rank_percent_rank"),
    )
)

stats = (
    with_windows_promotions_2
    .withColumn("discount_pct_dense_rank_2_percent_rank_tier", F.when(F.col("discount_pct_dense_rank_2_percent_rank") >= 0.78, "LARGE").otherwise("SMALL"))
    .withColumn("discount_pct_dense_rank_rank_percent_rank_tier", F.when(F.col("discount_pct_dense_rank_rank_percent_rank") >= 0.75, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["discount_pct_dense_rank_2_percent_rank_tier", "discount_pct_dense_rank_rank_percent_rank_tier"], ["discount_pct_dense_rank_2_percent_rank_tier"], []],
        "discount_pct_dense_rank_2_percent_rank_tier",
        "discount_pct_dense_rank_rank_percent_rank_tier",
    )
    .agg(
        F.round(F.avg("discount_pct_share"), 2).alias("avg_discount_pct_share"),
        F.percentile_approx("discount_pct_dense_rank_2_percent_rank", 0.95).alias("discount_pct_dense_rank_2_percent_rank_p95"),
        F.grouping("discount_pct_dense_rank_2_percent_rank_tier").alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "discount_pct_dense_rank_rank_percent_rank_tier",
        "avg_discount_pct_share",
        "discount_pct_dense_rank_2_percent_rank_p95",
    )
    .orderBy(F.col("avg_discount_pct_share").asc_nulls_last())
)
