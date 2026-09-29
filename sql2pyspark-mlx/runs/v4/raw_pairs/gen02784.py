from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.col("ship_to.city").alias("ship_city"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    unpacked
    .groupBy(
        "customer_id",
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        "ship_city",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("ship_city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count_if(F.col("ship_city") == "Berlin").alias("berlin_events"),
        F.count(F.when(F.col("ship_city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
    )
    .orderBy(F.col("chiang_mai_rows").desc())
)
