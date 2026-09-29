from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

result = lines.select("id", "created_at", "k", "value")
