from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isin([14, 20, 36]))
    .select(
        "id",
        "customer_id",
        "ship_to",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("ship_to.city").alias("ship_city"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)
