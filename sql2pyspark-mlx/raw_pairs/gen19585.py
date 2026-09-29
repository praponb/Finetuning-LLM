from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy(F.col("price").desc_nulls_first())
w3 = Window.orderBy(F.col("product_id").asc_nulls_last())

lines = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    lines
    .filter(F.col("customer_id") == 36)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "price",
        "promo",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.sum("product_id").over(w).alias("customer_id_sum_product_id"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
        F.cume_dist().over(w3).alias("product_id_cume_dist"),
        F.max("qty").over(w).alias("customer_id_max_qty"),
    )
)
