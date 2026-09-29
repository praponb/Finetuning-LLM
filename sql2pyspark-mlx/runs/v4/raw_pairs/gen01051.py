from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("promo")
w_ordered = Window.orderBy(F.col("price").desc_nulls_first())
w5 = Window.partitionBy("promo", "customer_id").orderBy(F.col("price_percent_rank").desc())
w6 = Window.partitionBy("customer_id").orderBy(F.col("price_share_2").desc_nulls_first())
w7 = Window.orderBy(F.col("price_dense_rank").desc())
w8 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_sum_price_dense_rank").asc_nulls_first())
w9 = Window.partitionBy("customer_id", "promo").orderBy(F.col("price").desc_nulls_first())

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", "customer_id", F.col("elem.price"), F.col("elem.promo"))
)

metrics = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share_2"),
        F.percent_rank().over(w_ordered).alias("price_percent_rank"),
    )
)

metrics_carts = (
    metrics
    .withColumn("customer_id_sum_price_dense_rank", F.sum("price_dense_rank").over(w2))
    .withColumn("price_percent_rank_rank", F.rank().over(w5))
    .withColumn("customer_id_stddev_samp_price_share_2", F.stddev_samp("price_share_2").over(w2))
    .withColumn("price_share_2_percent_rank", F.percent_rank().over(w6))
)

enriched = (
    metrics_carts
    .withColumn("price_dense_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("customer_id_sum_price_dense_rank_percent_rank", F.percent_rank().over(w8))
    .withColumn("price_percent_rank_2", F.percent_rank().over(w9))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("customer_id_sum_price_dense_rank") >= 44, "LARGE").otherwise("SMALL").alias("customer_id_sum_price_dense_rank_tier"),
    )
    .agg(
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
    )
    .orderBy(F.col("customer_id_sum_price_dense_rank_tier").desc_nulls_first())
)
