from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("value").orderBy(F.col("qty").desc())
w2 = Window.partitionBy("value")

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

exploded = (
    lines
    .select(
        "id",
        "pos",
        "product_id",
        "qty",
        "price",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

scored = (
    exploded
    .withColumn("qty_rank", F.rank().over(w_ordered))
    .withColumn("qty_dense_rank", F.dense_rank().over(w_ordered))
)

filtered = (
    scored
    .filter(F.col("price") >= 80)
    .select(
        "*",
        F.coalesce("qty_rank", F.lit(0)).alias("qty_rank_filled"),
        F.col("pos").cast("double").alias("pos_str"),
    )
)

result = (
    filtered
    .select(
        "product_id",
        "qty",
        "price",
        "value",
        "qty_rank",
        "qty_dense_rank",
        "qty_rank_filled",
        F.count("qty_rank_filled").over(w2).alias("value_count_qty_rank_filled"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
    )
)
