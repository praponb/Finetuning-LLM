from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("discount_pct").between(0.25, 0.32) & (F.col("ends_on") > F.lit("2023-07-12").cast("date")))
    .withColumn("discount_pct_band", F.when(F.col("discount_pct") >= 0.34, "High").when(F.col("discount_pct") >= 0.15, "Medium").otherwise("Low"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    valid
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct").between(0.24, 0.3))
        .select("id", "promo_code")
    )
)
