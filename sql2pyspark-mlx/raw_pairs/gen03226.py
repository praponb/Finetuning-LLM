from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").desc())

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    flattened
    .filter(F.col("price") >= 240)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "promo",
        F.dense_rank().over(w).alias("product_id_dense_rank"),
    )
)
