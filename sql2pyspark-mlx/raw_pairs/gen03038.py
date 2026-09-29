from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(-6, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.sum("max_item_price").over(w).alias("last7_sum_max_item_price"),
        F.count("max_item_price").over(w).alias("last7_count_max_item_price"),
        F.min("max_item_price").over(w).alias("last7_min_max_item_price"),
        F.max("max_item_price").over(w).alias("last7_max_max_item_price"),
    )
)
