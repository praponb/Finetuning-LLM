from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id")
w2 = Window.partitionBy("promo_code", "channel")
w3 = Window.partitionBy(F.date_trunc("week", "starts_on")).orderBy("promo_id", "id", "promo_id").rowsBetween(-6, -1)

valid = (
    df_promotions
    .filter((F.col("starts_on") < "2024-09-01") & (F.col("starts_on") > F.lit("2023-06-02").cast("date")))
    .select("*", F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"))
)

with_window = (
    valid
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    with_window
    .filter(F.year("ends_on") == 2025)
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "channel_filled",
        "promo_id",
        "promotion_discount_pct",
        F.lead("discount_pct", 2).over(w).alias("next_discount_pct"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w2)), 4).alias("promo_id_share"),
        F.avg("promotion_discount_pct").over(w3).alias("promotion_discount_pct_avg_6_prior"),
    )
)

filtered = (
    enriched
    .filter(~F.col("channel").eqNullSafe(F.lit("store")))
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "next_discount_pct",
        "promo_id_share",
        "promotion_discount_pct_avg_6_prior",
        F.when(F.col("channel").isin(["mobile", "partner"]), "Core").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "next_discount_pct",
        "resolved_channel",
    )
)
