from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id").desc_nulls_last())
w2 = Window.orderBy(F.col("price").desc_nulls_first())
w3 = Window.orderBy(F.col("product_id_dense_rank").desc_nulls_first())
w4 = Window.orderBy(F.col("price_rank").asc_nulls_last())
w5 = Window.orderBy(F.col("product_id_dense_rank").desc_nulls_last())

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select("id", "coupon_codes", F.col("item.product_id"), F.col("item.price"))
)

metrics = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("product_id_rank"),
        F.percent_rank().over(w2).alias("price_percent_rank"),
        F.rank().over(w2).alias("price_rank"),
        F.dense_rank().over(w).alias("product_id_dense_rank"),
    )
)

enriched = (
    metrics
    .withColumn("product_id_dense_rank_rank", F.rank().over(w3))
    .withColumn("price_rank_percent_rank", F.percent_rank().over(w4))
    .withColumn("product_id_dense_rank_percent_rank", F.percent_rank().over(w5))
)

valid = (
    enriched
    .filter(F.col("price") < 170)
    .select(
        "*",
        F.round((F.col("product_id_rank") * 100), 2).alias("product_id_rank_scaled"),
    )
)

grouped = (
    valid
    .groupBy("product_id")
    .agg(
        F.min("price").alias("min_price"),
        F.max("product_id").alias("max_product_id"),
        F.round(F.avg("product_id_rank_scaled"), 2).alias("avg_product_id_rank_scaled"),
    )
)

result = grouped.select("product_id", "min_price", "max_product_id")
