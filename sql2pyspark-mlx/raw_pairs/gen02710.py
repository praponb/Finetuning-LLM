from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("num_items", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("num_items").desc(), "id")

unpacked = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["gift"].alias("gift"),
        F.size("items").alias("num_items"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
    )
)

result = (
    unpacked
    .filter(F.col("max_item_price").isNotNull())
    .select(
        "id",
        "gift",
        "num_items",
        "weight_kg",
        F.max("num_items").over(w_ordered).alias("last7_max_num_items"),
        F.count("num_items").over(w_ordered).alias("last7_count_num_items"),
        F.avg("num_items").over(w_ordered).alias("last7_avg_num_items"),
        F.ntile(3).over(w2).alias("num_items_bucket"),
    )
)
