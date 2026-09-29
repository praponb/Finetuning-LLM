from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("bulk_lines").desc(), "id")
w3 = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.hour("created_at").between(6, 12))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
        F.size("items").alias("num_items"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.sum("bulk_lines").over(w).alias("customer_id_sum_bulk_lines"),
        F.ntile(10).over(w2).alias("bulk_lines_bucket"),
        F.sum("num_items").over(w3).alias("cumulative_num_items"),
    )
)
