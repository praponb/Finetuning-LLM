from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("created_at", "id")

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "coupons_text",
        F.row_number().over(w_recent).alias("row_seq"),
        F.row_number().over(w_recent).alias("row_seq_2"),
    )
)
