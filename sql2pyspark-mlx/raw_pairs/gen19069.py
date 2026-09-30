from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("price_rounded").desc_nulls_last())
w2 = Window.partitionBy("element").orderBy(F.col("price").asc_nulls_last())
w_grp = Window.orderBy(F.col("product_id").desc())
w_ordered = Window.orderBy(F.col("element_length").desc_nulls_first())
w5 = Window.partitionBy("element")
w6 = Window.partitionBy("element").orderBy(F.col("price_rounded_dense_rank").desc())

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

lines = (
    flattened
    .select("id", "product_id", "price", F.explode("coupon_codes").alias("element"))
)

base = (
    lines
    .filter(F.col("element").isNotNull() & (~F.col("element").eqNullSafe(F.lit("VIP"))))
    .select(
        "*",
        F.length("element").alias("element_length"),
        F.round("price", 1).alias("price_rounded"),
    )
)

enriched = (
    base
    .withColumn("price_rounded_dense_rank", F.dense_rank().over(w_running))
    .withColumn("price_dense_rank", F.dense_rank().over(w2))
    .withColumn("product_id_rank", F.rank().over(w_grp))
    .withColumn("element_length_cume_dist", F.cume_dist().over(w_ordered))
)

metrics = (
    enriched
    .select(
        "id",
        "product_id",
        "element_length",
        "price_dense_rank",
        "product_id_rank",
        "element_length_cume_dist",
        F.count("product_id").over(w5).alias("element_count_product_id"),
        F.rank().over(w6).alias("price_rounded_dense_rank_rank"),
    )
)

result = (
    metrics
    .select("price_dense_rank", "product_id_rank", "element_count_product_id")
    .orderBy(F.col("element_count_product_id").asc_nulls_last())
)
