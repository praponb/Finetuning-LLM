from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id", "promo")
w4 = Window.partitionBy("promo", "customer_id").orderBy(F.col("qty").asc_nulls_last())

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    lines
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.sum("product_id").over(w2).alias("customer_id_sum_product_id"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share"),
        F.dense_rank().over(w4).alias("qty_dense_rank"),
    )
)
