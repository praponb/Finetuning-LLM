from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc())

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    exploded
    .filter(F.col("qty") < 3)
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.dense_rank().over(w2).alias("qty_dense_rank"),
    )
)
