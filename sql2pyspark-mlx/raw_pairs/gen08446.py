from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("starts_on") < "2023-09-01") & (F.col("starts_on") > F.lit("2023-06-14").cast("date")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
    )
)

result = (
    filtered
    .groupBy("promo_code")
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.round(F.sum("days_since_ends_on") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("days_since_ends_on_per_welcome5"),
        F.percentile_approx("days_since_ends_on", [0.25, 0.5, 0.75]).alias("days_since_ends_on_quartiles"),
    )
)
