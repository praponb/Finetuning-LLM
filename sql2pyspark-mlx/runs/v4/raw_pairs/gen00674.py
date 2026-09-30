from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("promo", "customer_id").orderBy("qty")
w3 = Window.orderBy(F.col("product_id").asc_nulls_first())
w4 = Window.partitionBy("customer_id", "promo").orderBy(F.col("product_id").desc())

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

result = (
    exploded
    .withColumn("customer_id_max_product_id", F.max("product_id").over(w))
    .withColumn("qty_rank", F.rank().over(w2))
    .withColumn("product_id_percent_rank", F.percent_rank().over(w3))
    .withColumn("product_id_dense_rank", F.dense_rank().over(w4))
)
