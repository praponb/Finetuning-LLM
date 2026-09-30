from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.col("attributes")["device"].alias("device"),
        F.size("items").alias("num_items"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("cart_value") >= 1330, "PREMIUM").otherwise("STANDARD").alias("cart_value_tier"),
        "customer_id",
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("row_count"),
        F.max("created_at").alias("last_created_at"),
    )
)
