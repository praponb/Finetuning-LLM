from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("cart_value").desc(), "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy("cart_value", "id")
w4 = Window.partitionBy("customer_id").orderBy("bulk_lines", "id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "attributes",
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    parsed
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "cart_value",
        F.ntile(5).over(w).alias("cart_value_bucket"),
        F.stddev_samp("bulk_lines").over(w2).alias("customer_id_stddev_samp_bulk_lines"),
        F.row_number().over(w3).alias("seq_no"),
        F.lag("cart_value", 1).over(w4).alias("prev_cart_value"),
        F.lag("cart_value", 1).over(w3).alias("prev_cart_value_2"),
    )
)
