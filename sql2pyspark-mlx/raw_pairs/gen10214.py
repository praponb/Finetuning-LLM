from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("WEEK", "ends_on").alias("week_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max("discount_pct").alias("max_discount_pct"),
        F.max("discount_pct").alias("max_discount_pct_2"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct")).otherwise(0)).alias("bundle_discount_pct"),
    )
)
