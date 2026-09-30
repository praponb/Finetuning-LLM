from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("max_item_price").asc_nulls_first())

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "attributes",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.col("attributes")["gift"].alias("gift"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    unpacked
    .withColumn("next_max_item_price", F.lead("max_item_price", 1, 0).over(w))
    .withColumn("max_item_price_rank", F.rank().over(w2))
)
