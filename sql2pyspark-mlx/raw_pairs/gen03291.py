from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("promo")
w2 = Window.partitionBy("value")

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened_carts = (
    flattened
    .select("product_id", "price", "promo", F.explode("coupon_codes").alias("value"))
)

scored = (
    flattened_carts
    .filter(F.col("promo").isin(["WELCOME5", "VIP"]))
    .select(
        "product_id",
        "value",
        F.count("price").over(w_running).alias("promo_count_price"),
        F.sum("product_id").over(w_running).alias("promo_sum_product_id"),
    )
)

scored_carts = (
    scored
    .select(
        "value",
        "promo_sum_product_id",
        F.round((F.col("promo_sum_product_id") / F.sum("promo_sum_product_id").over(w2)), 4).alias("promo_sum_product_id_share"),
    )
)

valid = (
    scored_carts
    .filter(~F.col("value").eqNullSafe(F.lit("SPRING10")))
    .select(
        "*",
        F.when(F.col("value") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
        F.coalesce("value", F.lit("OTHER")).alias("value_filled"),
    )
)

scored_carts_2 = (
    valid
    .select(
        "value",
        "promo_sum_product_id_share",
        "is_spring10",
        F.round((F.col("promo_sum_product_id") / F.sum("promo_sum_product_id").over(w2)), 4).alias("promo_sum_product_id_share_2"),
        F.round((F.col("promo_sum_product_id") / F.sum("promo_sum_product_id").over(w2)), 4).alias("promo_sum_product_id_share_3"),
    )
)

valid_carts = (
    scored_carts_2
    .filter(F.col("promo_sum_product_id_share_2").between(0.24, 0.51))
    .select(
        "value",
        "promo_sum_product_id_share",
        "promo_sum_product_id_share_2",
        "promo_sum_product_id_share_3",
        F.least("promo_sum_product_id_share_3", "promo_sum_product_id_share_2").alias("least_promo_sum_product_id_share_3_promo_sum_product_id_share_2"),
        F.coalesce("value", F.lit("DIRECT")).alias("value_filled"),
    )
)

result = (
    valid_carts
    .groupBy("value")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("value") == "WELCOME5", 1)).alias("welcome5_count"),
        F.percentile_approx("promo_sum_product_id_share", 0.9).alias("promo_sum_product_id_share_p90"),
    )
)
