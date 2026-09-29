from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w_part = Window.partitionBy("promo_code").orderBy("discount_pct")
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("promotion_promo_code").orderBy(F.col("discount_pct_dense_rank").desc_nulls_last())
w5 = Window.orderBy("discount_pct_rank")

matched_keys = (
    df_promotions
    .select("promo_code", "channel")
    .subtract(
        df_promotions
        .filter(F.col("promo_code") != "WELCOME5")
        .select("promo_code", "channel")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.dense_rank().over(w_part).alias("discount_pct_dense_rank"),
        F.rank().over(w3).alias("discount_pct_rank"),
    )
)

matched = (
    enriched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    matched
    .withColumn("discount_pct_dense_rank_dense_rank", F.dense_rank().over(w4))
    .withColumn("discount_pct_rank_percent_rank", F.percent_rank().over(w5))
)

daily = (
    metrics
    .groupBy(
        F.when(F.col("promo_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("promo_id_tier"),
    )
    .agg(
        F.avg("discount_pct_rank").alias("avg_discount_pct_rank"),
        F.round(F.sum("promo_id") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("promo_id_per_mobile"),
        F.min("discount_pct_rank_percent_rank").alias("min_discount_pct_rank_percent_rank"),
    )
    .filter(F.col("avg_discount_pct_rank") < 0)
)

result = (
    daily
    .select(
        "promo_id_tier",
        "avg_discount_pct_rank",
        "min_discount_pct_rank_percent_rank",
    )
)
