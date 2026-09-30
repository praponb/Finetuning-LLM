from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.date_trunc("YEAR", "starts_on").alias("year_start"),
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
    )
    .orderBy(F.col("distinct_promo_codes").desc())
)
