from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("created_at"), "customer_id").orderBy("cart_value", "id")
w3 = Window.orderBy(F.col("cart_value").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy(F.to_date("created_at"), "customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .filter(F.to_date("created_at") < "2024-04-02")
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.sum("cart_value").over(w).alias("cumulative_cart_value"),
        F.lead("cart_value", 2).over(w2).alias("next_cart_value"),
        F.ntile(3).over(w3).alias("cart_value_bucket"),
        F.count("cart_value").over(w4).alias("cart_value_count_1h"),
        F.sum("cart_value").over(w5).alias("cumulative_cart_value_2"),
    )
)
