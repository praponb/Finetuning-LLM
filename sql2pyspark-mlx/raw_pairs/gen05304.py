from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .filter(F.col("num_items") >= 2)
    .select(
        "id",
        "weight_kg",
        "max_item_price",
        F.sum("num_items").over(w).alias("running_sum_num_items"),
        F.min("num_items").over(w).alias("running_min_num_items"),
        F.max("num_items").over(w).alias("running_max_num_items"),
    )
)
