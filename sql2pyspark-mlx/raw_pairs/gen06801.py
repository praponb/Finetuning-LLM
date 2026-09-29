from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("bulk_lines", "id").rowsBetween(-2, -1)

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "bulk_lines",
        "has_welcome_code",
        "used_welcome5",
        F.count("bulk_lines").over(w).alias("running_count_bulk_lines"),
        F.sum("bulk_lines").over(w).alias("running_sum_bulk_lines"),
        F.min("bulk_lines").over(w).alias("running_min_bulk_lines"),
        F.avg("bulk_lines").over(w2).alias("bulk_lines_avg_2_prior"),
    )
)
