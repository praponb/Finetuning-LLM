from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id")
w_recent = Window.orderBy("num_attributes", "id").rowsBetween(-2, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.size("coupon_codes") > 2)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["gift"].alias("gift"),
        F.size("attributes").alias("num_attributes"),
        F.col("attributes").getItem("device").alias("device"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    unpacked
    .withColumn("next_num_attributes", F.lead("num_attributes", 1).over(w))
    .withColumn("num_attributes_avg_2", F.avg("num_attributes").over(w_recent))
)
