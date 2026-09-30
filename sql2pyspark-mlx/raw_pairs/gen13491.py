from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct")
w2 = Window.partitionBy(F.date_trunc("WEEK", "starts_on"))
w3 = Window.orderBy(F.col("starts_on_period_count_discount_pct").desc_nulls_first())
w4 = Window.orderBy(F.col("discount_pct_rank").asc_nulls_first())
w5 = Window.orderBy(F.col("discount_pct_rank_dense_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("discount_pct_rank_dense_rank").asc_nulls_last())
w7 = Window.partitionBy(F.year("ends_on"))
w8 = Window.orderBy(F.col("starts_on_period_count_discount_pct").desc_nulls_last())
w9 = Window.orderBy(F.col("discount_pct_rank_dense_rank").desc())

matched_keys = (
    df_promotions
    .select("promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("starts_on").between("2023-08-23", "2024-08-31"))
        .select("promo_code")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.year("starts_on") >= 2025)
    .select(
        "id",
        "starts_on",
        "ends_on",
        F.rank().over(w).alias("discount_pct_rank"),
        F.count("discount_pct").over(w2).alias("starts_on_period_count_discount_pct"),
    )
)

matched_keys_2 = (
    metrics
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") <= 0.28)
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.percent_rank().over(w3).alias("starts_on_period_count_discount_pct_percent_rank"),
        F.dense_rank().over(w4).alias("discount_pct_rank_dense_rank"),
    )
)

enriched_promotions = (
    enriched
    .select(
        "*",
        F.rank().over(w5).alias("discount_pct_rank_dense_rank_rank"),
        F.rank().over(w6).alias("discount_pct_rank_dense_rank_rank_2"),
        F.stddev_samp("starts_on_period_count_discount_pct_percent_rank").over(w7).alias("ends_on_year_stddev_samp_starts_on_period_count_discount_pct_percent_rank"),
        F.percent_rank().over(w8).alias("starts_on_period_count_discount_pct_percent_rank_2"),
        F.percent_rank().over(w9).alias("discount_pct_rank_dense_rank_percent_rank"),
    )
)

result = (
    enriched_promotions
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct_rank",
        "starts_on_period_count_discount_pct_percent_rank",
        "discount_pct_rank_dense_rank",
        "ends_on_year_stddev_samp_starts_on_period_count_discount_pct_percent_rank",
        "discount_pct_rank_dense_rank_percent_rank",
    )
    .orderBy("starts_on")
)
