from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") < "2023-11-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.size("items").alias("num_items"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
    )
)
