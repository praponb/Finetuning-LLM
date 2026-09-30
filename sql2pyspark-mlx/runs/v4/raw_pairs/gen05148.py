from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-06-16"))) & (F.col("channel") == "store"))
    .select(
        "*",
        F.concat("channel", F.lit(", "), "promo_code").alias("channel_promo_code"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
