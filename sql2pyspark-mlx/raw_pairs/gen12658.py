from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "coupon_codes",
        F.col("ship_to.city").alias("ship_city"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    unpacked
    .groupBy("ship_city")
    .agg(
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
        F.size(F.collect_set("ship_city")).alias("num_ship_cities"),
    )
)
