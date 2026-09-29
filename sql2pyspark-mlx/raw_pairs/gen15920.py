from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("bulk_lines", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("items_total", "id").rowsBetween(-5, -1)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
        F.col("attributes").getItem("device").alias("device"),
    )
)

result = (
    parsed
    .select(
        "id",
        "device",
        F.count("items_total").over(w).alias("last7_count_items_total"),
        F.max("items_total").over(w).alias("last7_max_items_total"),
        F.avg("items_total").over(w).alias("last7_avg_items_total"),
        F.avg("bulk_lines").over(w2).alias("bulk_lines_avg_5_prior"),
    )
)
