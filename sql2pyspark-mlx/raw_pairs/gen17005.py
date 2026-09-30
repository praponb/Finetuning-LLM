from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["BUNDLE", "WELCOME5"])
    .agg(F.avg("discount_pct").alias("value_sum"))
    .select(
        "channel",
        F.col("BUNDLE").alias("bundle"),
        F.col("WELCOME5").alias("welcome5"),
    )
)
