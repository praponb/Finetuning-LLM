from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc_nulls_first())
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("qty").asc_nulls_first())
w4 = Window.orderBy(F.col("product_id").desc_nulls_first())

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

metrics = (
    flattened
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("customer_id_stddev_samp_price", F.stddev_samp("price").over(w))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w2))
)

scored = (
    metrics
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share_2"),
        F.dense_rank().over(w_ordered).alias("qty_dense_rank"),
        F.dense_rank().over(w4).alias("product_id_dense_rank"),
    )
)

result = (
    scored
    .rollup("customer_id")
    .agg(
        F.percentile_approx("product_id", 0.9).alias("product_id_p90"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("product_id_cume_dist", 0.9).alias("product_id_cume_dist_p90"),
    )
    .filter(F.col("distinct_customer_ids") < 1)
)
