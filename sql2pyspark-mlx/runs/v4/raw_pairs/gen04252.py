from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("ship_to.country").alias("ship_country"),
    )
)
