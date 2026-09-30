from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "ship_to",
        F.col("ship_to.city").alias("ship_city"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("items_total") >= 1220, "HIGH").otherwise("LOW").alias("items_total_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("items_total") / F.nullif(F.count(F.when(F.col("ship_city") == "Tokyo", 1)), F.lit(0)), 2).alias("items_total_per_tokyo"),
        F.count_if(F.col("ship_city") == "Chiang Mai").alias("chiang_mai_events"),
    )
)
