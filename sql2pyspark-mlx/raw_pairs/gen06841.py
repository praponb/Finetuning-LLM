from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("weight_kg").desc(), "id")
w2 = Window.orderBy("weight_kg", "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "weight_kg",
        "has_premium_item",
        F.ntile(10).over(w).alias("weight_kg_bucket"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
