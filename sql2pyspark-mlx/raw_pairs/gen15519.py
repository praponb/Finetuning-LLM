from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())
w_hist = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.orderBy(F.col("val").desc())
w4 = Window.orderBy("val")
w5 = Window.orderBy(F.col("val_rank").asc_nulls_first())
w6 = Window.orderBy(F.col("val_cume_dist").desc())
w7 = Window.orderBy(F.col("val_rank_2").asc_nulls_first())
w8 = Window.orderBy(F.col("val_dense_rank").asc_nulls_last())

lines = df_price_tiers.select("id", F.explode("tier_limits").alias("val"))

scored = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("val_rank"),
        F.cume_dist().over(w_hist).alias("val_cume_dist"),
        F.cume_dist().over(w3).alias("val_cume_dist_2"),
    )
)

scored_price_tiers = (
    scored
    .withColumn("val_rank_2", F.rank().over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w4))
    .withColumn("val_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("val_cume_dist_dense_rank", F.dense_rank().over(w6))
)

metrics = (
    scored_price_tiers
    .filter(F.col("val_rank_percent_rank") < 0.62)
    .select(
        "id",
        "val",
        "val_rank",
        "val_cume_dist",
        "val_cume_dist_2",
        "val_rank_2",
        "val_dense_rank",
        F.rank().over(w).alias("val_rank_3"),
    )
)

result = (
    metrics
    .withColumn("val_rank_2_dense_rank", F.dense_rank().over(w7))
    .withColumn("val_dense_rank_rank", F.rank().over(w8))
)
