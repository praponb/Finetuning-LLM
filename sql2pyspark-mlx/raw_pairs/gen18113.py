from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct_rounded").desc_nulls_last())
w3 = Window.partitionBy(F.date_trunc("week", "ends_on"), "channel")

kept_keys = (
    df_promotions
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") < 0.32)
        .select("id", "channel")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-06-09")))
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
        F.substring("promo_code", 1, 3).alias("promo_code_prefix"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        "discount_pct_rounded",
        "promo_code_prefix",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.rank().over(w2).alias("discount_pct_rounded_rank"),
        F.stddev_samp("discount_pct_rounded").over(w3).alias("ends_on_period_stddev_samp_discount_pct_rounded"),
    )
)

matched = (
    metrics
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

pivoted = (
    matched
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "WELCOME5", "VIP"])
    .agg(F.sum("discount_pct"))
)

result = (
    pivoted
    .select(
        "channel",
        "SPRING10",
        "WELCOME5",
        "VIP",
        F.when(F.col("channel").rlike("^(store|web)$"), "CORE").otherwise("GROUP_A").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc())
)
