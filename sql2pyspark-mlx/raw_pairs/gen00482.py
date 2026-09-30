from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_carts
    .filter(F.size("coupon_codes") >= 2)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "coupons_text",
        F.count("bulk_lines").over(w_recent).alias("running_count_bulk_lines"),
        F.sum("bulk_lines").over(w_recent).alias("running_sum_bulk_lines"),
        F.max("bulk_lines").over(w_recent).alias("running_max_bulk_lines"),
    )
)
