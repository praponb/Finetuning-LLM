from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy(F.col("product_id").desc())
w2 = Window.orderBy(F.col("price").desc())
w3 = Window.partitionBy("customer_id", "code").orderBy(F.col("price").desc_nulls_last())
w4 = Window.partitionBy("code")
w5 = Window.partitionBy("code").orderBy(F.col("code_stddev_samp_product_id_dense_rank_rounded").desc())

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.price"),
    )
)

lines_carts = (
    lines
    .select(
        "customer_id",
        "product_id",
        "price",
        F.explode("coupon_codes").alias("code"),
    )
)

scored = (
    lines_carts
    .select(
        "*",
        F.dense_rank().over(w).alias("product_id_dense_rank"),
        F.rank().over(w2).alias("price_rank"),
        F.dense_rank().over(w3).alias("price_dense_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("code").isNotNull())
    .select(
        "product_id",
        "price",
        "code",
        "product_id_dense_rank",
        "price_rank",
        "price_dense_rank",
        F.trim("code").alias("clean_code"),
        F.round("product_id_dense_rank", 1).alias("product_id_dense_rank_rounded"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.stddev_samp("product_id_dense_rank_rounded").over(w4).alias("code_stddev_samp_product_id_dense_rank_rounded"),
        F.min("price_dense_rank").over(w4).alias("code_min_price_dense_rank"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.stddev_samp("price").over(w4).alias("code_stddev_samp_price"),
        F.percent_rank().over(w5).alias("code_stddev_samp_product_id_dense_rank_rounded_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "product_id",
        "price",
        "code",
        "price_dense_rank",
        "clean_code",
        "code_min_price_dense_rank",
        "code_stddev_samp_product_id_dense_rank_rounded_percent_rank",
    )
    .orderBy(F.col("price_dense_rank").desc_nulls_last())
)
