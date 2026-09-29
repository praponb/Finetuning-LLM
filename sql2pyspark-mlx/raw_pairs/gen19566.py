from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
    )
)
