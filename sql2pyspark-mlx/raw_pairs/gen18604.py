from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
    )
)
