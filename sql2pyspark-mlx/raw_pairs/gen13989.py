from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
    )
)
