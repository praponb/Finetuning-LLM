from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("MONTH", "created_at"), "customer_id").orderBy(F.col("cart_value").desc(), "id")

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.size("items").alias("num_items"),
        F.col("attributes")["gift"].alias("gift"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
    )
)

result = (
    unpacked
    .filter(F.col("cart_value").between(800, 1400))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "num_items",
        "gift",
        "cart_value",
        F.sum("cart_value").over(w).alias("cart_value_sum_30m"),
        F.count("cart_value").over(w2).alias("cart_value_count_15m"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
