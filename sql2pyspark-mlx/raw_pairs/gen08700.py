from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-05-31"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"),
        F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"),
    )
)

result = (
    filtered
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .cube("year_start")
    .agg(
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct")).otherwise(0)).alias("bundle_discount_pct"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("discount_pct_per_bundle"),
        F.count("*").alias("row_count"),
        F.grouping("year_start").alias("grouping_id"),
    )
    .orderBy("bundle_discount_pct")
)
