from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("id")
    .intersect(
        df_promotions
        .filter(F.month("starts_on").isin([1, 5, 12]))
        .select("id")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("discount_pct") >= 0.29, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.count("*").alias("num_rows_2"),
    )
)
