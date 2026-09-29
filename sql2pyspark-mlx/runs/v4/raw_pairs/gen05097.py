from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-10-05").cast("date")) & F.col("promo_code").isin(["BUNDLE", "WELCOME5", "VIP"]))
    .select(
        "*",
        F.date_trunc("MONTH", "starts_on").alias("starts_on_period"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .groupBy(
        "channel",
        F.when(F.col("discount_pct") >= 0.3, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        "promo_code",
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.min("starts_on_period").alias("first_seen"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
    )
)
