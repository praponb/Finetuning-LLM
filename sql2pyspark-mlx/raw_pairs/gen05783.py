from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.col("attributes")["device"].alias("device"),
    )
)
