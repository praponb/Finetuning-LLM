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
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.size("attributes").alias("num_attributes"),
    )
)
