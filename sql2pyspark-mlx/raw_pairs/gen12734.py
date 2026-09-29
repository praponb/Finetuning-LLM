from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("max_item_price", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("customer_id").orderBy(F.col("num_attributes").desc_nulls_last())

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.size("attributes").alias("num_attributes"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    parsed
    .filter(F.col("max_item_price") >= 150)
    .select(
        "id",
        "coupon_codes",
        "max_item_price",
        F.avg("num_attributes").over(w_part).alias("num_attributes_avg_2_prior"),
        F.rank().over(w2).alias("num_attributes_rank"),
    )
)
