from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("YEAR", "ends_on").alias("year_start"))
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.max(F.when(F.col("promo_code") == "VIP", F.col("discount_pct"))).alias("vip_max_discount_pct"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct")).otherwise(0)).alias("welcome5_discount_pct"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.max("discount_pct").alias("max_discount_pct"),
    )
)
