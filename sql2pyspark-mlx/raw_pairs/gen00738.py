from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("price").desc())
w3 = Window.partitionBy(F.to_date("created_at"), "promo")
w4 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy(F.to_date("created_at"))
w6 = Window.orderBy(F.col("price").desc_nulls_last())

lines = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

scored = (
    lines
    .withColumn("customer_id_count_qty", F.count("qty").over(w))
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
)

metrics = (
    scored
    .filter(F.col("customer_id_count_qty") >= 10)
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "qty",
        "price",
        "promo",
        "product_id_share",
        F.dense_rank().over(w2).alias("price_dense_rank"),
        F.stddev_samp("customer_id_count_qty").over(w3).alias("created_at_to_date_stddev_samp_customer_id_count_qty"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.price",
        "s.promo",
        "s.product_id_share",
        "s.price_dense_rank",
        "s.created_at_to_date_stddev_samp_customer_id_count_qty",
        F.col("d.segment"),
        F.col("d.country"),
        F.col("d.signup_date"),
    )
)

with_windows = (
    enriched
    .select(
        "created_at",
        "price",
        "promo",
        "product_id_share",
        "segment",
        "country",
        F.count("price_dense_rank").over(w4).alias("price_dense_rank_count_2h"),
        F.max("price").over(w5).alias("created_at_to_date_max_price"),
        F.cume_dist().over(w6).alias("price_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "created_at",
        "promo",
        "product_id_share",
        "segment",
        "country",
        "price_dense_rank_count_2h",
        "price_cume_dist",
    )
    .orderBy(F.col("segment").desc(), F.col("price_dense_rank_count_2h").desc())
)
