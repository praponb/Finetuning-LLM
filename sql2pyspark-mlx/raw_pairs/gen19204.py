from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_first())
w2 = Window.orderBy(F.col("qty_minus_price").desc())
w3 = Window.orderBy(F.col("qty_minus_price_cume_dist").asc_nulls_last())
w4 = Window.orderBy(F.col("qty_minus_price_cume_dist").desc_nulls_last())
w5 = Window.orderBy(F.col("qty_minus_price").asc_nulls_last())
w6 = Window.orderBy(F.col("qty_minus_price_cume_dist_dense_rank").desc_nulls_first())
w7 = Window.orderBy(F.col("qty_minus_price_rank").desc())
w8 = Window.orderBy(F.col("qty_minus_price_cume_dist_percent_rank").desc_nulls_first())
w9 = Window.orderBy(F.col("qty_minus_price_cume_dist").asc_nulls_first())

flattened = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

filtered = (
    flattened
    .filter(F.col("product_id").between(4, 8))
    .select(
        "coupon_codes",
        "qty",
        F.array_contains("coupon_codes", "BUNDLE").alias("is_bundle"),
        (F.col("qty") - F.col("price")).alias("qty_minus_price"),
    )
)

valid = (
    filtered
    .filter(F.col("qty") >= 4)
    .withColumn("is_bundle_flag", F.col("is_bundle").cast("int"))
)

scored = (
    valid
    .select(
        "coupon_codes",
        "is_bundle",
        "qty_minus_price",
        "is_bundle_flag",
        F.percent_rank().over(w).alias("qty_percent_rank"),
        F.cume_dist().over(w2).alias("qty_minus_price_cume_dist"),
    )
)

metrics = (
    scored
    .filter(F.col("qty_minus_price") > 36)
    .select(
        "coupon_codes",
        "qty_percent_rank",
        "qty_minus_price_cume_dist",
        F.percent_rank().over(w3).alias("qty_minus_price_cume_dist_percent_rank"),
        F.dense_rank().over(w4).alias("qty_minus_price_cume_dist_dense_rank"),
        F.rank().over(w5).alias("qty_minus_price_rank"),
    )
)

enriched = (
    metrics
    .withColumn("qty_minus_price_cume_dist_dense_rank_percent_rank", F.percent_rank().over(w6))
    .withColumn("qty_minus_price_rank_cume_dist", F.cume_dist().over(w7))
    .withColumn("qty_minus_price_cume_dist_percent_rank_percent_rank", F.percent_rank().over(w8))
    .withColumn("qty_minus_price_cume_dist_rank", F.rank().over(w9))
)

result = (
    enriched
    .select(
        "qty_minus_price_cume_dist",
        "qty_minus_price_rank",
        "qty_minus_price_cume_dist_dense_rank_percent_rank",
        "qty_minus_price_cume_dist_percent_rank_percent_rank",
        "qty_minus_price_cume_dist_rank",
    )
    .orderBy("qty_minus_price_rank")
)
