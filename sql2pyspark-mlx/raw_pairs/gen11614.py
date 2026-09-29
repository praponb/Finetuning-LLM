from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.col("promo_code").isin(["WELCOME5", "SPRING10", "VIP"]))
        .select("id")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("discount_pct") >= 0.35, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
