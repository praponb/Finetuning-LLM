from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.size("items").alias("num_items"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
    )
)
