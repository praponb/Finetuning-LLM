from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.size("coupon_codes") > 2)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.percentile_approx("num_attributes", [0.25, 0.5, 0.75]).alias("num_attributes_quartiles"),
        F.round(F.avg("num_attributes"), 2).alias("avg_num_attributes"),
        F.percentile_approx("num_attributes", 0.95).alias("num_attributes_p95"),
        F.sum("num_attributes").alias("total_num_attributes"),
    )
)
