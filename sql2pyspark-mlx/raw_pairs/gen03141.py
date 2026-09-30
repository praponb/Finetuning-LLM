from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").asc_nulls_last())
w2 = Window.orderBy(F.col("element").desc())
w3 = Window.orderBy("element")
w4 = Window.orderBy(F.col("element").desc_nulls_last())
w_hist = Window.orderBy(F.col("element_cume_dist_2").desc_nulls_last())
w6 = Window.orderBy(F.col("element_percent_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("element_percent_rank_percent_rank").desc())
w8 = Window.orderBy(F.col("element_dense_rank").desc_nulls_first())
w9 = Window.orderBy("element_percent_rank")
w10 = Window.orderBy(F.col("element_dense_rank").desc_nulls_last())

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("element"))

with_windows = (
    exploded
    .select(
        "*",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank_2"),
        F.cume_dist().over(w4).alias("element_cume_dist_2"),
    )
)

remaining_keys = (
    with_windows
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") == "starter")
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("element_cume_dist_2_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("element_percent_rank_percent_rank", F.percent_rank().over(w6))
    .withColumn("element_rank", F.rank().over(w))
)

metrics = (
    scored
    .select(
        "id",
        "element",
        "element_dense_rank",
        "element_percent_rank",
        "element_cume_dist_2",
        "element_rank",
        F.rank().over(w7).alias("element_percent_rank_percent_rank_rank"),
        F.rank().over(w8).alias("element_dense_rank_rank"),
        F.rank().over(w9).alias("element_percent_rank_rank"),
        F.dense_rank().over(w10).alias("element_dense_rank_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "element_percent_rank",
        "element_cume_dist_2",
        "element_rank",
        "element_dense_rank_rank",
        "element_percent_rank_rank",
        "element_dense_rank_dense_rank",
    )
    .orderBy(F.col("element_cume_dist_2").asc_nulls_first())
)
