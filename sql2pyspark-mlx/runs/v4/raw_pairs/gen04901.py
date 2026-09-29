from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("ship_city") == "Chiang Mai").alias("chiang_mai_events"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("hour_window").asc_nulls_last())
)
