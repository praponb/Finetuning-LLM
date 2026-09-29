from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.hour("created_at").between(10, 18))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.col("attributes")["gift"].alias("gift"),
        F.col("ship_to.city").alias("ship_city"),
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)
