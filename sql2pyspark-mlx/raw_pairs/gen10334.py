from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.size("coupon_codes") > 2)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.when(F.col("items_total") >= 1560, "HIGH").otherwise("LOW").alias("items_total_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("created_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") >= 1)
)
