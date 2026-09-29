from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("items_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.to_date("created_at") >= "2025-03-01")
    .select(
        "id",
        "created_at",
        "items",
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.col("attributes")["gift"].alias("gift"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    unpacked
    .filter(F.col("items_total") < 800)
    .select(
        "id",
        "created_at",
        "items_total",
        "gift",
        F.count("items_total").over(w_ordered).alias("running_count_items_total"),
        F.sum("items_total").over(w_ordered).alias("running_sum_items_total"),
        F.avg("items_total").over(w_ordered).alias("running_avg_items_total"),
        F.max("items_total").over(w_ordered).alias("running_max_items_total"),
    )
)
