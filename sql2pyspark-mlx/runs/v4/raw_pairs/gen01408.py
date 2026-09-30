from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "ship_country").orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("ship_city")
w3 = Window.partitionBy("ship_country")

unpacked = (
    df_carts
    .filter(F.col("customer_id").isin([13, 25, 37]))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        F.col("ship_to.country").alias("ship_country"),
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes").getItem("gift").alias("gift"),
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .withColumn("cumulative_num_items", F.sum("num_items").over(w))
    .withColumn("ship_city_sum_num_items", F.sum("num_items").over(w_running))
    .withColumn("ship_country_count_num_items", F.count("num_items").over(w3))
)
