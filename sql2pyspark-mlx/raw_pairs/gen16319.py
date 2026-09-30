from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isin([8, 13]))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)
