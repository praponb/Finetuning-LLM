from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("promo_code") == "SPRING10"))
    .withColumn("starts_on_period", F.date_trunc("MONTH", "starts_on"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("NONE")))
)

result = (
    valid
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
