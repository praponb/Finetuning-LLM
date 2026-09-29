from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("discount_pct", "id", "promo_id")
w2 = Window.partitionBy("year_start").orderBy(F.col("discount_pct_stddev").desc())

filtered = (
    df_promotions
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

matched = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    matched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "channel_clean",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
    )
)

with_window = (
    latest
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

summary = (
    with_window
    .groupBy(
        F.date_trunc("YEAR", "starts_on").alias("year_start"),
        F.when(F.col("promo_id_2") >= 10, "PREMIUM").otherwise("STANDARD").alias("promo_id_2_tier"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
    )
    .withColumn("discount_pct_stddev_rank", F.rank().over(w2))
)

result = (
    summary
    .select(
        "year_start",
        "promo_id_2_tier",
        "partner_events",
        "discount_pct_stddev_rank",
    )
    .orderBy(F.col("partner_events").desc())
)
