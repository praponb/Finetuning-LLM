from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.34, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        F.date_trunc("YEAR", "starts_on").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_rows"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct")).otherwise(0)).alias("spring10_discount_pct"),
    )
    .filter(F.col("event_count") < 1)
)
