from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        F.col("ship_to.country").alias("ship_country"),
        F.size("attributes").alias("num_attributes"),
    )
)
