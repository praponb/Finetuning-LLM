from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.size("coupon_codes") > 1)
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.col("ship_to.city").alias("ship_city"),
    )
)
