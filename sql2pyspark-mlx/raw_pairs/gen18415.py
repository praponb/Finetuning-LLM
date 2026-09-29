from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "max_item_price",
        "coupons_text",
        F.sum("max_item_price").over(w).alias("running_sum_max_item_price"),
        F.max("max_item_price").over(w).alias("running_max_max_item_price"),
        F.avg("max_item_price").over(w).alias("running_avg_max_item_price"),
    )
)
