from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc_nulls_first())
w2 = Window.orderBy(F.col("price").desc())
w3 = Window.partitionBy("value", "promo")
w4 = Window.partitionBy("segment")
w5 = Window.partitionBy("promo", "segment")
w6 = Window.partitionBy("value")
w7 = Window.partitionBy("promo")

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

enriched = (
    flattened
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("a.*", F.col("c.segment"), F.col("c.country"), F.col("c.signup_date"))
)

metrics = (
    enriched
    .filter(F.col("promo").isin(["SPRING10", "BUNDLE", "VIP"]))
    .select(
        "price",
        "promo",
        "value",
        "segment",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.percent_rank().over(w2).alias("price_percent_rank"),
        F.dense_rank().over(w).alias("price_dense_rank_2"),
    )
)

valid = (
    metrics
    .filter(F.col("value") == "WELCOME5")
    .select(
        "promo",
        "value",
        "segment",
        "price_dense_rank",
        "price_percent_rank",
        "price_dense_rank_2",
        F.upper("promo").alias("promo_upper"),
        (F.col("price_dense_rank") / F.nullif("price_percent_rank", F.lit(0))).alias("price_dense_rank_per_price_percent_rank"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("resolved_promo"),
    )
)

metrics_carts = (
    valid
    .withColumn("value_count_price_percent_rank", F.count("price_percent_rank").over(w3))
    .withColumn("segment_max_price_dense_rank_per_price_percent_rank", F.max("price_dense_rank_per_price_percent_rank").over(w4))
    .withColumn("promo_max_price_percent_rank", F.max("price_percent_rank").over(w5))
    .withColumn("segment_count_price_dense_rank_per_price_percent_rank", F.count("price_dense_rank_per_price_percent_rank").over(w4))
    .withColumn("value_sum_price_dense_rank_2", F.sum("price_dense_rank_2").over(w6))
)

with_windows = (
    metrics_carts
    .select(
        "*",
        F.max("value_sum_price_dense_rank_2").over(w6).alias("value_max_value_sum_price_dense_rank_2"),
        F.min("promo_max_price_percent_rank").over(w7).alias("promo_min_promo_max_price_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "value",
        "segment",
        "price_dense_rank",
        "promo_upper",
        "price_dense_rank_per_price_percent_rank",
        "resolved_promo",
        "promo_max_price_percent_rank",
        "segment_count_price_dense_rank_per_price_percent_rank",
        "value_sum_price_dense_rank_2",
    )
    .orderBy(
        F.col("price_dense_rank").desc_nulls_last(),
        F.col("price_dense_rank_per_price_percent_rank").desc(),
    )
)
