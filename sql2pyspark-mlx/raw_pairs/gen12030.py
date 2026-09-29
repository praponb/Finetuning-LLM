from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        F.size("attributes").alias("num_attributes"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("cart_value") >= 1680, "PREMIUM").otherwise("STANDARD").alias("cart_value_tier"),
    )
    .agg(
        F.percentile_approx("cart_value", [0.25, 0.5, 0.75]).alias("cart_value_quartiles"),
        F.percentile_approx("num_attributes", [0.25, 0.5, 0.75]).alias("num_attributes_quartiles"),
        F.count("*").alias("cnt"),
        F.percentile_approx("num_attributes", 0.5).alias("num_attributes_p50"),
    )
    .filter(F.col("num_attributes_p50") < 1000000)
    .orderBy("cnt")
)
