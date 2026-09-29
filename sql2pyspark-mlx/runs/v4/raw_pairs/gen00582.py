from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["BUNDLE", "WELCOME5", "SPRING10"])
    .agg(F.max("discount_pct").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "channel",
        F.col("BUNDLE_total").alias("bundle_total"),
        F.col("BUNDLE_item_count").alias("bundle_item_count"),
        F.col("WELCOME5_total").alias("welcome5_total"),
        F.col("WELCOME5_item_count").alias("welcome5_item_count"),
        F.col("SPRING10_total").alias("spring10_total"),
        F.col("SPRING10_item_count").alias("spring10_item_count"),
    )
)
