from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("max_item_price", "id")
w2 = Window.partitionBy("customer_id")

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id").isin([20, 33, 40]))
    .select(
        "id",
        "customer_id",
        "ship_to",
        F.col("attributes").getItem("gift").alias("gift"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("next_max_item_price", F.lead("max_item_price", 2).over(w_recent))
    .withColumn("max_item_price_bucket", F.ntile(5).over(w_recent))
    .withColumn("max_item_price_share", F.round((F.col("max_item_price") / F.sum("max_item_price").over(w2)), 4))
)
