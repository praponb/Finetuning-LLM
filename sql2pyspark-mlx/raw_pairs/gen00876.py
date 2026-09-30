from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    unpacked
    .groupBy("ship_city", F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.min("created_at").alias("started_at"),
        F.count(F.when(F.col("ship_city") == "London", 1)).alias("london_count"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("london_count") > 1)
)
