from pyspark.sql import functions as F

matched = (
    df_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
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
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
    )
    .orderBy(
        F.col("promotion_discount_pct").asc_nulls_first(),
        F.col("promo_id").desc(),
    )
)
