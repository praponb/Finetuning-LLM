from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("promo_code") == "VIP")
    .select(
        "*",
        F.concat("channel", F.lit(" "), "promo_code").alias("channel_promo_code"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

result = (
    cleaned
    .groupBy("channel")
    .pivot("promo_code", ["VIP", "WELCOME5", "SPRING10"])
    .agg(F.max("discount_pct").alias("total"), F.count(F.lit(1)).alias("n"))
)
