from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

result = flattened.select("id", "coupon_codes", "v")
