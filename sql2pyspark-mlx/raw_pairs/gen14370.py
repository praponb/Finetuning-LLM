from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = lines.select("id", "coupon_codes", "attr_key", "attr_value")
