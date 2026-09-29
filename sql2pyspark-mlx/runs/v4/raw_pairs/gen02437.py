from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.hour("created_at").between(11, 19))
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    unpacked
    .groupBy("customer_id", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("started_at"),
        F.max("created_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") > 0)
)
