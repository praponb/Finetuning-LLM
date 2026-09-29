from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("product_id").desc_nulls_last())
w_ordered = Window.orderBy(F.col("price").desc_nulls_first())
w4 = Window.partitionBy("promo").orderBy("position")
w5 = Window.orderBy(F.col("position_filled").desc_nulls_last())
w_running = Window.orderBy(F.col("product_id_dense_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("position").desc_nulls_first())

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("li_item.product_id"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

matched_keys = (
    flattened
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

verified = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("product_id").between(7, 8))
    .select(
        "id",
        "position",
        "product_id",
        "price",
        "promo",
        F.max("product_id").over(w_hist).alias("promo_max_product_id"),
        F.stddev_samp("price").over(w_hist).alias("promo_stddev_samp_price"),
        F.dense_rank().over(w2).alias("product_id_dense_rank"),
        F.rank().over(w_ordered).alias("price_rank"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "position",
        "product_id",
        "promo_stddev_samp_price",
        "product_id_dense_rank",
        "price_rank",
        F.cume_dist().over(w4).alias("position_cume_dist"),
    )
)

base = (
    scored
    .filter(F.col("promo_stddev_samp_price").between(165, 174))
    .select("*", F.coalesce("position", F.lit(0)).alias("position_filled"))
)

base_carts = (
    base
    .filter(F.col("position_cume_dist") <= 0.34)
    .select(
        "id",
        "position",
        "promo_stddev_samp_price",
        "product_id_dense_rank",
        "position_filled",
        F.col("product_id_dense_rank").cast("string").alias("product_id_dense_rank_num"),
        F.when(F.col("promo_stddev_samp_price") >= 264, "High").when(F.col("promo_stddev_samp_price") >= 53, "Medium").otherwise("Low").alias("promo_stddev_samp_price_level"),
    )
)

with_windows_carts = (
    base_carts
    .select(
        "id",
        "promo_stddev_samp_price",
        "product_id_dense_rank",
        "position_filled",
        "promo_stddev_samp_price_level",
        F.percent_rank().over(w5).alias("position_filled_percent_rank"),
        F.dense_rank().over(w_running).alias("product_id_dense_rank_dense_rank"),
        F.dense_rank().over(w7).alias("position_dense_rank"),
    )
)

result = (
    with_windows_carts
    .select(
        "promo_stddev_samp_price",
        "promo_stddev_samp_price_level",
        "position_filled_percent_rank",
        "position_dense_rank",
    )
    .orderBy("promo_stddev_samp_price")
)
