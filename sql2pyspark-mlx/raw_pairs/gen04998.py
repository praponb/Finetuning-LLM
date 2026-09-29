from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())

valid = (
    df_promotions
    .filter(F.col("starts_on") > F.to_date(F.lit("2023-04-30")))
    .select(
        "*",
        F.substring("promo_code", 1, 2).alias("promo_code_prefix"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

with_window = (
    valid
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

base = (
    with_window
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-06-15"))) & F.col("promotion_promo_code").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_code_prefix",
        "channel_filled",
        "promo_id",
        "promotion_discount_pct",
        F.greatest("promo_id", "promotion_discount_pct").alias("greatest_promo_id_promotion_discount_pct"),
        F.round("promo_id", 2).alias("promo_id_rounded"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "channel",
        "starts_on",
        "promo_code_prefix",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_rounded",
    )
)

result = (
    deduped
    .select("id", "channel", "promo_id", "promotion_discount_pct", "promo_id_rounded")
    .orderBy(F.col("channel").desc_nulls_last())
)
