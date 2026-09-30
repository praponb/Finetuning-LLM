from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w_part = Window.partitionBy("plan").orderBy(F.col("element").desc())
w4 = Window.partitionBy(F.date_trunc("YEAR", "effective_from")).orderBy(F.col("element").desc_nulls_last())
w5 = Window.orderBy(F.col("plan_count_element").asc_nulls_last())
w6 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("element").asc_nulls_first())
w7 = Window.partitionBy("plan").orderBy(F.col("element_percent_rank").desc_nulls_last())
w8 = Window.orderBy(F.col("plan_min_element_dense_rank").asc_nulls_first())
w9 = Window.orderBy(F.col("plan_min_element_dense_rank").desc_nulls_first())
w10 = Window.orderBy("plan_min_element_percent_rank")
w11 = Window.orderBy(F.col("plan_count_element_cume_dist").desc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

metrics = (
    flattened
    .filter(F.month("effective_from").isin([5, 8, 11]))
    .select(
        "*",
        F.count("element").over(w_recent).alias("plan_count_element"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.sum("element").over(w_recent).alias("plan_sum_element"),
        F.percent_rank().over(w_part).alias("element_percent_rank"),
        F.cume_dist().over(w4).alias("element_cume_dist"),
    )
)

metrics_price_tiers = (
    metrics
    .select(
        "plan_count_element",
        "element_dense_rank",
        "plan_sum_element",
        F.min("element_dense_rank").over(w_recent).alias("plan_min_element_dense_rank"),
        F.cume_dist().over(w5).alias("plan_count_element_cume_dist"),
        F.min("element_percent_rank").over(w_recent).alias("plan_min_element_percent_rank"),
        F.percent_rank().over(w6).alias("element_percent_rank_2"),
        F.cume_dist().over(w7).alias("element_percent_rank_cume_dist"),
    )
)

filtered = (
    metrics_price_tiers
    .filter(F.col("element_dense_rank") == 5)
    .select(
        "*",
        F.ceil("plan_min_element_percent_rank").alias("plan_min_element_percent_rank_whole"),
    )
)

enriched = (
    filtered
    .select(
        "plan_count_element_cume_dist",
        "plan_min_element_percent_rank_whole",
        F.cume_dist().over(w8).alias("plan_min_element_dense_rank_cume_dist"),
        F.rank().over(w5).alias("plan_count_element_rank"),
        F.dense_rank().over(w9).alias("plan_min_element_dense_rank_dense_rank"),
        F.percent_rank().over(w10).alias("plan_min_element_percent_rank_percent_rank"),
        F.percent_rank().over(w11).alias("plan_count_element_cume_dist_percent_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("plan_min_element_percent_rank_whole") >= 62)
    .withColumn("plan_min_element_dense_rank_cume_dist_per_plan_count_element_cume_dist", (F.col("plan_min_element_dense_rank_cume_dist") / F.nullif("plan_count_element_cume_dist", F.lit(0))))
)

base = (
    cleaned
    .filter(F.col("plan_count_element_rank") == 6)
    .select(
        "plan_count_element_cume_dist",
        "plan_min_element_percent_rank_whole",
        "plan_count_element_rank",
        "plan_min_element_percent_rank_percent_rank",
        "plan_count_element_cume_dist_percent_rank",
        (F.col("plan_count_element_cume_dist_percent_rank") * F.col("plan_min_element_percent_rank_whole")).alias("plan_count_element_cume_dist_percent_rank_times_plan_min_element_percent_rank_whole"),
    )
)

result = (
    base
    .select(
        "plan_count_element_cume_dist",
        "plan_count_element_rank",
        "plan_count_element_cume_dist_percent_rank",
        "plan_count_element_cume_dist_percent_rank_times_plan_min_element_percent_rank_whole",
    )
    .orderBy("plan_count_element_cume_dist")
)
