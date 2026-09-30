from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("num_items", "id").rowsBetween(-2, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "ship_to",
        F.size("items").alias("num_items"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    parsed
    .filter(F.col("has_premium_item"))
    .select(
        "*",
        F.count("num_items").over(w_ordered).alias("last3_count_num_items"),
        F.max("num_items").over(w_ordered).alias("last3_max_num_items"),
        F.sum("num_items").over(w_ordered).alias("last3_sum_num_items"),
        F.avg("num_items").over(w_ordered).alias("last3_avg_num_items"),
    )
)
