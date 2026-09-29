from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("starts_on"), "promotion_promo_code")
w2 = Window.orderBy("discount_pct")
w3 = Window.orderBy(F.col("promotion_discount_pct").desc_nulls_first())
w4 = Window.partitionBy("promotion_promo_code")

remaining_keys = (
    df_promotions
    .select("channel", "id")
    .intersect(
        df_promotions
        .filter(F.col("ends_on") <= "2023-11-01")
        .select("channel", "id")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_window = (
    verified
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    with_window
    .select(
        "*",
        F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w)), 4).alias("promotion_discount_pct_share"),
        F.rank().over(w2).alias("discount_pct_rank"),
        F.rank().over(w3).alias("promotion_discount_pct_rank"),
    )
)

with_windows = (
    enriched
    .filter(F.col("promo_code") == "SPRING10")
    .withColumn("promotion_discount_pct_share_2", F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w4)), 4))
    .withColumn("promotion_promo_code_stddev_samp_promotion_discount_pct_share", F.stddev_samp("promotion_discount_pct_share").over(w4))
)

with_window_promotions = (
    with_windows
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

base = (
    with_window_promotions
    .filter(F.col("discount_pct_rank") == 8)
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promotion_discount_pct_share",
        "discount_pct_rank",
        "promotion_discount_pct_rank",
        "promotion_discount_pct_share_2",
        "promotion_promo_code_stddev_samp_promotion_discount_pct_share",
        "promo_id_2",
        "promotion_discount_pct_2",
        F.round((F.col("discount_pct_rank") * 1.15), 2).alias("discount_pct_rank_adjusted"),
    )
)

result = (
    base
    .select(
        "id",
        "promo_code",
        "starts_on",
        "promotion_promo_code",
        "promotion_discount_pct_rank",
        "promotion_discount_pct_share_2",
        "promo_id_2",
        "promotion_discount_pct_2",
        "discount_pct_rank_adjusted",
    )
)
