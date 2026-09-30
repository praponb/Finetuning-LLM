from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("starts_on") >= F.to_date(F.lit("2024-04-08")))
    .select(
        "*",
        F.concat("channel", F.lit(", "), "promo_code").alias("channel_promo_code"),
        F.concat_ws("-", "promo_code", "channel").alias("promo_code_channel_key"),
    )
)

result = (
    filtered
    .groupBy("promo_code")
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.max(F.when(F.col("channel") == "mobile", F.col("discount_pct"))).alias("mobile_max_discount_pct"),
    )
)
