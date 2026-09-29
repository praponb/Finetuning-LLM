from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("element").desc_nulls_first())
w_ordered = Window.orderBy(F.col("element").desc())
w3 = Window.orderBy("element_percent_rank")
w4 = Window.orderBy(F.col("element_dense_rank").desc_nulls_first())
w_running = Window.orderBy(F.col("element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

enriched = (
    lines
    .filter(F.col("effective_from") < "2024-12-01")
    .select(
        "id",
        "effective_from",
        F.percent_rank().over(w_hist).alias("element_percent_rank"),
        F.dense_rank().over(w_ordered).alias("element_dense_rank"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "element_percent_rank",
        "element_dense_rank",
        F.dense_rank().over(w3).alias("element_percent_rank_dense_rank"),
    )
)

scored = metrics.withColumn("element_dense_rank_cume_dist", F.cume_dist().over(w4))

cleaned = (
    scored
    .filter(F.col("element_dense_rank_cume_dist").between(0.4, 0.46))
    .select(
        "id",
        "element_percent_rank",
        "element_percent_rank_dense_rank",
        (F.col("element_percent_rank_dense_rank") + F.col("element_dense_rank_cume_dist")).alias("element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist"),
    )
)

scored_price_tiers = (
    cleaned
    .select(
        "*",
        F.rank().over(w_running).alias("element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist_rank"),
    )
)

cleaned_price_tiers = (
    scored_price_tiers
    .filter(F.col("element_percent_rank_dense_rank") == 7)
    .select(
        "id",
        "element_percent_rank",
        "element_percent_rank_dense_rank",
        "element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist",
        "element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist_rank",
        F.col("element_percent_rank").cast("int").alias("element_percent_rank_int"),
    )
)

result = (
    cleaned_price_tiers
    .select(
        "id",
        "element_percent_rank",
        "element_percent_rank_dense_rank",
        "element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist",
        "element_percent_rank_int",
    )
    .orderBy(F.col("id").desc())
)
