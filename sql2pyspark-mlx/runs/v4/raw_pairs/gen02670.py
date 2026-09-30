from pyspark.sql import functions as F

attributed = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "channel",
        "starts_on",
        "promo_id",
        "promotion_discount_pct",
        F.when(F.col("channel").rlike("^(mobile|partner)$"), "EXTENDED").when(F.col("channel").rlike("^(web|store)$"), "CORE").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("promo_id").desc_nulls_first(), "channel")
)
