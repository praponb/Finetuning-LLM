from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("months_since_ends_on").desc_nulls_last())
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy(F.date_trunc("week", "ends_on"))
w4 = Window.partitionBy(F.date_trunc("MONTH", "ends_on"), "promo_code")
w5 = Window.partitionBy(F.month("ends_on"), "promo_code").orderBy("months_since_ends_on_rank_share")
w6 = Window.partitionBy(F.year("ends_on"), "promo_code")

filtered = (
    df_promotions
    .filter((F.col("channel").eqNullSafe(F.lit("web"))) & (F.col("channel") == "web"))
    .withColumn("adjusted_discount_pct", F.round((F.col("discount_pct") * 0.85), 2))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("NONE")))
)

filtered_promotions = (
    filtered
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "channel_clean",
        (F.datediff(F.current_date(), "ends_on") / 30).alias("months_since_ends_on"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean_2"),
    )
)

remaining_keys = (
    filtered_promotions
    .select("promo_code", "id")
    .exceptAll(
        df_promotions
        .filter(F.col("promo_code").isin(["WELCOME5", "VIP"]))
        .select("promo_code", "id")
    )
)

kept_rows = (
    filtered_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("promo_code") == "BUNDLE")
    .select(
        "id",
        "promo_code",
        "ends_on",
        "channel_clean_2",
        F.rank().over(w).alias("months_since_ends_on_rank"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)

metrics = (
    scored
    .select(
        "promo_code",
        "ends_on",
        "channel_clean_2",
        "months_since_ends_on_rank",
        "discount_pct_share",
        F.round((F.col("months_since_ends_on_rank") / F.sum("months_since_ends_on_rank").over(w2)), 4).alias("months_since_ends_on_rank_share"),
        F.max("discount_pct_share").over(w3).alias("ends_on_period_max_discount_pct_share"),
    )
)

scored_promotions = (
    metrics
    .select(
        "*",
        F.count("months_since_ends_on_rank_share").over(w4).alias("ends_on_period_count_months_since_ends_on_rank_share"),
        F.count("discount_pct_share").over(w2).alias("promo_code_count_discount_pct_share"),
        F.dense_rank().over(w5).alias("months_since_ends_on_rank_share_dense_rank"),
        F.stddev_samp("ends_on_period_max_discount_pct_share").over(w6).alias("ends_on_year_stddev_samp_ends_on_period_max_discount_pct_share"),
        F.stddev_samp("months_since_ends_on_rank").over(w2).alias("promo_code_stddev_samp_months_since_ends_on_rank"),
    )
)

result = (
    scored_promotions
    .select(
        "promo_code",
        "ends_on",
        "channel_clean_2",
        "months_since_ends_on_rank",
        "discount_pct_share",
        "months_since_ends_on_rank_share",
        "ends_on_period_max_discount_pct_share",
        "ends_on_period_count_months_since_ends_on_rank_share",
        "promo_code_count_discount_pct_share",
    )
    .orderBy(F.col("promo_code").desc())
)
