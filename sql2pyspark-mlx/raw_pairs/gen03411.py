from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("customer_id")
w_recent = Window.partitionBy("customer_id", "promo")
w4 = Window.orderBy(F.col("customer_id_max_pos").asc_nulls_last())
w5 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_max_pos").desc_nulls_first())
w6 = Window.orderBy(F.col("pos").desc_nulls_first())
w7 = Window.partitionBy("customer_id").orderBy("customer_id_max_pos")
w8 = Window.partitionBy("city").orderBy(F.col("customer_id_max_pos_cume_dist_2_int").desc_nulls_last())
w9 = Window.partitionBy("city")

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

metrics = (
    flattened
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
    .withColumn("customer_id_count_price", F.count("price").over(w2))
    .withColumn("customer_id_max_pos", F.max("pos").over(w_recent))
)

metrics_carts = (
    metrics
    .withColumn("customer_id_max_pos_dense_rank", F.dense_rank().over(w4))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w_recent)), 4))
)

metrics_carts_2 = (
    metrics_carts
    .filter(F.col("price") < 80)
    .select(
        "id",
        "customer_id",
        "price",
        "promo",
        "customer_id_count_price",
        "customer_id_max_pos",
        "customer_id_max_pos_dense_rank",
        "price_share",
        F.cume_dist().over(w5).alias("customer_id_max_pos_cume_dist"),
        F.cume_dist().over(w6).alias("pos_cume_dist"),
        F.cume_dist().over(w7).alias("customer_id_max_pos_cume_dist_2"),
        F.avg("customer_id_max_pos").over(w_recent).alias("customer_id_avg_customer_id_max_pos"),
        F.stddev_samp("product_id_share").over(w2).alias("customer_id_stddev_samp_product_id_share"),
    )
)

valid = (
    metrics_carts_2
    .filter(F.col("promo").isNotNull())
    .withColumn("customer_id_max_pos_cume_dist_2_int", F.col("customer_id_max_pos_cume_dist_2").cast("int"))
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "s.customer_id",
        "s.promo",
        "s.customer_id_count_price",
        "s.customer_id_max_pos_dense_rank",
        "s.customer_id_max_pos_cume_dist_2_int",
        F.col("c.email"),
        F.col("c.city"),
    )
)

result = (
    enriched
    .select(
        "customer_id",
        "promo",
        "customer_id_count_price",
        "customer_id_max_pos_dense_rank",
        "city",
        F.dense_rank().over(w8).alias("customer_id_max_pos_cume_dist_2_int_dense_rank"),
        F.round((F.col("customer_id_count_price") / F.sum("customer_id_count_price").over(w9)), 4).alias("customer_id_count_price_share"),
    )
)
