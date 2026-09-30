from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(~F.col("channel").isin(["partner", "mobile", "store"]))
    .select(
        "*",
        F.date_add("ends_on", 3).alias("ends_on_plus_3d"),
        F.last_day("starts_on").alias("starts_on_month_end"),
    )
)

result = (
    base
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on_plus_3d") >= F.col("pr.starts_on")) & (F.col("a.ends_on_plus_3d") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
