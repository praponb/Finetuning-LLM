from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value", "promo")
w2 = Window.partitionBy("value")
w3 = Window.partitionBy("promo").orderBy("price")
w4 = Window.orderBy(F.col("price_share_2").desc_nulls_first())
w5 = Window.orderBy(F.col("price").desc_nulls_first())
w6 = Window.orderBy(F.col("price_share").desc_nulls_last())
w7 = Window.partitionBy("promo")

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened = lines.select("price", "promo", F.explode("coupon_codes").alias("value"))

metrics = (
    flattened
    .filter(F.col("value") != "VIP")
    .select(
        "price",
        "promo",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.min("price").over(w2).alias("value_min_price"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share_2"),
    )
)

filtered = (
    metrics
    .filter(F.col("price").between(80, 100))
    .select(
        "price",
        "promo",
        "price_share",
        "value_min_price",
        "price_share_2",
        F.length("promo").alias("promo_length"),
        (F.col("price_share") * F.col("price")).alias("price_share_times_price"),
    )
)

metrics_carts = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w3).alias("price_percent_rank"),
        F.rank().over(w4).alias("price_share_2_rank"),
        F.cume_dist().over(w5).alias("price_cume_dist"),
        F.rank().over(w6).alias("price_share_rank"),
        F.round((F.col("price") / F.sum("price").over(w7)), 4).alias("price_share_3"),
    )
)

filtered_carts = (
    metrics_carts
    .filter(F.col("promo").isNotNull())
    .select(
        "price",
        "promo",
        "price_share",
        "value_min_price",
        "price_share_2",
        "price_percent_rank",
        "price_cume_dist",
        "price_share_rank",
        F.round((F.col("price_share_rank") * 100), 2).alias("price_share_rank_adjusted"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("resolved_promo"),
    )
)

filtered_carts_2 = (
    filtered_carts
    .filter(F.col("promo").isNotNull())
    .withColumn("price_share_2_minus_price_share_rank_adjusted", (F.col("price_share_2") - F.col("price_share_rank_adjusted")))
    .withColumn("value_min_price_rounded", F.round("value_min_price", 0))
    .withColumn("promo_clean", F.coalesce("promo", F.lit("DIRECT")))
)

result = (
    filtered_carts_2
    .select(
        "promo",
        F.col("price_share_2_minus_price_share_rank_adjusted").alias("PRICE_SHARE_2_MINUS_PRICE_SHARE_RANK_ADJUSTED_VAL"),
        F.col("price_share").alias("PRICE_SHARE"),
        F.col("price").alias("PRICE"),
    )
    .unpivot(
        ids=["promo"],
        values=["PRICE_SHARE_2_MINUS_PRICE_SHARE_RANK_ADJUSTED_VAL", "PRICE_SHARE", "PRICE"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
