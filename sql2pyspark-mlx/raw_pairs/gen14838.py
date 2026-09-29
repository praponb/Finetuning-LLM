from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "attributes",
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
    )
)
