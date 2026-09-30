from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)
