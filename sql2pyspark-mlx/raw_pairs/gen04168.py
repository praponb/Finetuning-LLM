from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id").desc())
w2 = Window.orderBy("product_id")
w3 = Window.orderBy(F.col("product_id_dense_rank").desc_nulls_first())
w4 = Window.orderBy(F.col("product_id").desc_nulls_first())
w5 = Window.orderBy(F.col("product_id").desc_nulls_last())
w6 = Window.orderBy(F.col("product_id_dense_rank_3").desc_nulls_last())
w7 = Window.orderBy(F.col("product_id_rank").desc())

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", F.col("elem.product_id"), F.col("elem.qty"))
)

metrics = lines.select("*", F.dense_rank().over(w).alias("product_id_dense_rank"))

enriched = (
    metrics
    .select(
        "id",
        "product_id",
        "qty",
        F.dense_rank().over(w2).alias("product_id_dense_rank_2"),
        F.rank().over(w).alias("product_id_rank"),
        F.cume_dist().over(w3).alias("product_id_dense_rank_cume_dist"),
        F.rank().over(w4).alias("product_id_rank_2"),
        F.dense_rank().over(w5).alias("product_id_dense_rank_3"),
    )
)

scored = (
    enriched
    .filter(F.col("qty") >= 2)
    .select(
        "*",
        F.rank().over(w6).alias("product_id_dense_rank_3_rank"),
        F.cume_dist().over(w7).alias("product_id_rank_cume_dist"),
    )
)

result = (
    scored
    .select(
        "product_id",
        "qty",
        "product_id_rank",
        "product_id_rank_2",
        "product_id_dense_rank_3",
        "product_id_dense_rank_3_rank",
        "product_id_rank_cume_dist",
    )
)
