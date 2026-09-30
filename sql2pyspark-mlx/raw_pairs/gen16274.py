from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "created_at",
        "max_item_price",
        F.last("max_item_price", ignorenulls=True).over(w_running).alias("last_known_max_item_price"),
        F.sum("max_item_price").over(w_running).alias("cumulative_max_item_price"),
    )
)
