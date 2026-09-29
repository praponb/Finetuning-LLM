from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("price").desc_nulls_first())
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy(F.col("price").desc_nulls_last())

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

result = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share_2"),
        F.cume_dist().over(w4).alias("price_cume_dist"),
    )
)
