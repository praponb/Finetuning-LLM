from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "created_at"), "customer_id")
w2 = Window.orderBy("max_item_price", "id")
w3 = Window.partitionBy(F.month("created_at"), "customer_id").orderBy(F.col("max_item_price").desc(), "id")
w4 = Window.partitionBy(F.month("created_at"), "customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes")["gift"], F.lit("none")).alias("gift_or_default"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "coupon_codes",
        F.round((F.col("max_item_price") / F.sum("max_item_price").over(w)), 4).alias("max_item_price_share"),
        F.lead("max_item_price", 2).over(w2).alias("next_max_item_price"),
        F.round((F.col("max_item_price") / F.sum("max_item_price").over(w)), 4).alias("max_item_price_share_2"),
        F.ntile(4).over(w3).alias("max_item_price_bucket"),
        F.sum("max_item_price").over(w4).alias("running_max_item_price"),
    )
)
