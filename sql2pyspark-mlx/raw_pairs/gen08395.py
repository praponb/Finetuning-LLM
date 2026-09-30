from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code").orderBy("promo_id")
w3 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("promo_id_share").desc())
w4 = Window.partitionBy(F.date_trunc("WEEK", "starts_on"), "channel")
w5 = Window.partitionBy(F.month("starts_on"), "channel")
w6 = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct").desc_nulls_first())

attributed = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

remaining_keys = (
    attributed
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on") < "2025-02-01")
        .select("id", "channel")
    )
)

kept_rows = (
    attributed
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("promo_id_share", F.round((F.col("promo_id") / F.sum("promo_id").over(w)), 4))
    .withColumn("channel_avg_discount_pct", F.avg("discount_pct").over(w))
    .withColumn("promo_id_percent_rank", F.percent_rank().over(w2))
)

with_windows = (
    scored
    .select(
        "channel",
        "promo_code",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promo_id_share",
        "channel_avg_discount_pct",
        "promo_id_percent_rank",
        F.cume_dist().over(w3).alias("promo_id_share_cume_dist"),
        F.max("channel_avg_discount_pct").over(w4).alias("starts_on_period_max_channel_avg_discount_pct"),
        F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w5)), 4).alias("promotion_discount_pct_share"),
        F.percent_rank().over(w6).alias("promotion_discount_pct_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        "promo_id",
        F.when(F.col("starts_on_period_max_channel_avg_discount_pct") >= 0, "LARGE").otherwise("SMALL").alias("starts_on_period_max_channel_avg_discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("starts_on_period_max_channel_avg_discount_pct", [0.25, 0.5, 0.75]).alias("starts_on_period_max_channel_avg_discount_pct_quartiles"),
        F.sum(F.when(F.col("promotion_promo_code") == "VIP", F.col("promotion_discount_pct_share")).otherwise(0)).alias("vip_promotion_discount_pct_share"),
        F.max("starts_on_period_max_channel_avg_discount_pct").alias("max_starts_on_period_max_channel_avg_discount_pct"),
    )
)
