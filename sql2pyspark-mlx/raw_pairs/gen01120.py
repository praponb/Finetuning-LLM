from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-11-18").cast("date"))
    .withColumn("promo_code_group", F.when(F.col("promo_code").isin(["SPRING10", "VIP"]), "Tier 1").otherwise("Other"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    cleaned
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_pct_per_web"),
    )
    .filter(F.col("web_count") < 1000)
)
