from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("qty").asc_nulls_first())
w3 = Window.orderBy(F.col("qty_dense_rank").asc_nulls_first())
w4 = Window.orderBy(F.col("qty_dense_rank").desc_nulls_first())
w5 = Window.orderBy(F.col("product_id").desc_nulls_last())
w_recent = Window.orderBy(F.col("product_id").asc_nulls_last())

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

with_dims = (
    flattened
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.customer_id", "e.product_id", "e.qty", F.col("p.email"))
)

scored = (
    with_dims
    .select(
        "product_id",
        "qty",
        F.dense_rank().over(w).alias("product_id_dense_rank"),
        F.dense_rank().over(w2).alias("qty_dense_rank"),
    )
)

enriched = (
    scored
    .withColumn("qty_dense_rank_percent_rank", F.percent_rank().over(w3))
    .withColumn("qty_dense_rank_cume_dist", F.cume_dist().over(w4))
    .withColumn("product_id_dense_rank_2", F.dense_rank().over(w5))
    .withColumn("product_id_rank", F.rank().over(w_recent))
)

result = (
    enriched
    .rollup("product_id")
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("product_id_dense_rank"), 2).alias("avg_product_id_dense_rank"),
        F.percentile_approx("qty_dense_rank_percent_rank", [0.25, 0.5, 0.75]).alias("qty_dense_rank_percent_rank_quartiles"),
        F.percentile_approx("qty_dense_rank_cume_dist", 0.9).alias("qty_dense_rank_cume_dist_p90"),
    )
)
