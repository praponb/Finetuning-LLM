from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "attributes",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
    )
)
