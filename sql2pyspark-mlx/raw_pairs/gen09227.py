from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("max_item_price", "id")

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "coupon_codes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("attributes")["device"].alias("device"),
        F.size("attributes").alias("num_attributes"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "ship_zip",
        F.count("max_item_price").over(w).alias("running_count_max_item_price"),
        F.avg("max_item_price").over(w).alias("running_avg_max_item_price"),
        F.sum("max_item_price").over(w).alias("running_sum_max_item_price"),
        F.ntile(3).over(w2).alias("max_item_price_bucket"),
    )
)
