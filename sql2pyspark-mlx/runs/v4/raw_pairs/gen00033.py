from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("max_item_price", "id")
w2 = Window.partitionBy("customer_id").orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        F.col("attributes")["gift"].alias("gift"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.ntile(10).over(w).alias("max_item_price_bucket"),
        F.first("max_item_price", ignorenulls=True).over(w2).alias("first_known_max_item_price"),
        F.stddev_samp("max_item_price").over(w3).alias("customer_id_stddev_samp_max_item_price"),
        F.ntile(10).over(w).alias("max_item_price_bucket_2"),
    )
)
