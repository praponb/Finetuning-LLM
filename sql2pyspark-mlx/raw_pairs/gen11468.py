from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value")
w2 = Window.partitionBy("promo")
w3 = Window.orderBy("product_id_share_2_int")
w4 = Window.orderBy(F.col("product_id_share_2_minus_product_id").desc())
w5 = Window.orderBy(F.col("product_id_share_2").desc())
w6 = Window.orderBy(F.col("product_id_share_2_minus_product_id_percent_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("value_min_product_id").asc_nulls_last())
w8 = Window.orderBy(F.col("product_id_share_2_int_dense_rank_share").desc())

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

lines = (
    exploded
    .select("id", "product_id", "promo", F.explode("coupon_codes").alias("value"))
)

scored = (
    lines
    .select(
        "product_id",
        "promo",
        "value",
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.round((F.col("product_id") / F.sum("product_id").over(w2)), 4).alias("product_id_share_2"),
    )
)

valid = (
    scored
    .filter(F.col("product_id_share") > 0.34)
    .withColumn("product_id_share_2_int", F.col("product_id_share_2").cast("int"))
    .withColumn("value_clean", F.coalesce("value", F.lit("OTHER")))
)

valid_carts = (
    valid
    .filter(F.col("promo") != "SPRING10")
    .select(
        "product_id",
        "promo",
        "value",
        "product_id_share_2",
        "product_id_share_2_int",
        (F.col("product_id_share_2") - F.col("product_id")).alias("product_id_share_2_minus_product_id"),
        F.coalesce("value", F.lit("UNKNOWN")).alias("value_filled"),
    )
)

with_windows = (
    valid_carts
    .withColumn("product_id_share_2_int_dense_rank", F.dense_rank().over(w3))
    .withColumn("promo_min_product_id_share_2_int", F.min("product_id_share_2_int").over(w2))
    .withColumn("value_min_product_id", F.min("product_id").over(w))
)

with_windows_carts = (
    with_windows
    .select(
        "product_id_share_2_int",
        "value_filled",
        "value_min_product_id",
        F.percent_rank().over(w4).alias("product_id_share_2_minus_product_id_percent_rank"),
        F.percent_rank().over(w5).alias("product_id_share_2_percent_rank"),
        F.round((F.col("product_id_share_2_int_dense_rank") / F.sum("product_id_share_2_int_dense_rank").over(w)), 4).alias("product_id_share_2_int_dense_rank_share"),
    )
)

result = (
    with_windows_carts
    .select(
        "product_id_share_2_minus_product_id_percent_rank",
        "product_id_share_2_int_dense_rank_share",
        F.dense_rank().over(w6).alias("product_id_share_2_minus_product_id_percent_rank_dense_rank"),
        F.dense_rank().over(w7).alias("value_min_product_id_dense_rank"),
        F.rank().over(w8).alias("product_id_share_2_int_dense_rank_share_rank"),
    )
)
