from pyspark.sql import functions as F

matched = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        F.when(F.col("channel").rlike("^(web|mobile)$"), "GROUP_A").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("discount_pct").desc())
)
