from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") >= "2024-04-01")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.size("attributes").alias("num_attributes"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.col("ship_to.country").alias("ship_country"),
    )
)
