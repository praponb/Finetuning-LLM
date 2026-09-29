from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_city")
w2 = Window.partitionBy("ship_city").orderBy("num_items")
w3 = Window.partitionBy("ship_city").orderBy("num_items", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("ship_city").orderBy("created_at", "id")
w5 = Window.partitionBy("ship_city").orderBy("num_items", "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        F.size("items").alias("num_items"),
        F.col("ship_to.city").alias("ship_city"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("attributes")["gift"].alias("gift"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.avg("num_items").over(w).alias("ship_city_avg_num_items"),
        F.percent_rank().over(w2).alias("num_items_percent_rank"),
        F.avg("num_items").over(w3).alias("num_items_avg_6_prior"),
        F.lead("num_items", 1).over(w4).alias("next_num_items"),
        F.lag("num_items", 1).over(w5).alias("prev_num_items"),
    )
)
