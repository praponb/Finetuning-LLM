from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("num_items", "id")
w2 = Window.orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "ship_to",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .filter(F.col("num_items") <= 2)
    .select(
        "id",
        "customer_id",
        "has_welcome_code",
        "num_items",
        F.ntile(10).over(w).alias("num_items_bucket"),
        F.sum("num_items").over(w2).alias("cumulative_num_items"),
    )
)
