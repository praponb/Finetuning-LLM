from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.count("*").alias("row_count"),
        F.max("created_at").alias("last_seen"),
        F.max("created_at").alias("last_seen_2"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy("activity_day")
)
