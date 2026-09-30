from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-12-16").cast("date")) & F.col("channel").isNotNull())
    .withColumn("discount_pct_rounded", F.round("discount_pct", 2))
    .withColumn("starts_on_year", F.year("starts_on"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("discount_pct_rounded") >= 78, "LARGE").otherwise("SMALL").alias("discount_pct_rounded_tier"),
        F.when(F.col("starts_on_year") >= 69, "HIGH").otherwise("LOW").alias("starts_on_year_tier"),
    )
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("discount_pct_per_spring10"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .orderBy(F.col("discount_pct_per_spring10").desc())
)
