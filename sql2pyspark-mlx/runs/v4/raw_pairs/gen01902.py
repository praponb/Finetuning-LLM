from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.date_trunc("WEEK", "ends_on").alias("week_start"),
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
    )
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_pct_per_web"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.max(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct"))).alias("welcome5_max_discount_pct"),
    )
    .filter(F.col("welcome5_max_discount_pct") < 0)
    .orderBy("discount_pct_per_web")
)
