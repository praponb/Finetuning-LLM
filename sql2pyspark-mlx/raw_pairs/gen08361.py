from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("channel", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("starts_on") < "2025-07-27")
        .select("channel", "id")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("discount_pct") >= 0.33, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct")).otherwise(0)).alias("spring10_discount_pct"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount_pct")).otherwise(0)).alias("web_discount_pct"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount_pct")).otherwise(0)).alias("web_discount_pct_2"),
    )
    .filter(F.col("web_discount_pct") < 1000000)
)
