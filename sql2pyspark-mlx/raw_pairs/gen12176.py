from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("channel")
    .intersect(
        df_promotions
        .filter(F.col("ends_on") < "2023-11-01")
        .select("channel")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("discount_pct") >= 0.36, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.avg(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct"))).alias("spring10_avg_discount_pct"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_per_partner"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount_pct")).otherwise(0)).alias("web_discount_pct"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
    )
)
