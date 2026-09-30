from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.col("attributes").getItem("gift").alias("gift"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.col("attributes")["device"].alias("device"),
    )
)
