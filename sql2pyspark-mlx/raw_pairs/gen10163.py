from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    unpacked
    .groupBy("customer_id", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("ship_city") == "Bangkok", 1)).alias("bangkok_count"),
        F.count_if(F.col("ship_city") == "New York").alias("new_york_events"),
        F.count_if(F.col("ship_city") == "London").alias("london_events"),
        F.count(F.when(F.col("ship_city") == "New York", 1)).alias("new_york_count"),
    )
    .orderBy(F.col("bangkok_count").asc_nulls_first())
)
