from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.col("created_at") >= "2023-10-01")
    .select(
        "id",
        "created_at",
        "ship_to",
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.city").alias("ship_city"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    parsed
    .groupBy("ship_city")
    .agg(
        F.count(F.when(F.col("ship_city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.countDistinct("device").alias("distinct_devices"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes_2"),
        F.max("created_at").alias("ended_at"),
    )
    .filter(F.col("bangkok_rows") >= 1)
)
