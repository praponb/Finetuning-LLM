from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("currency")

lines = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("element"))

scored = (
    lines
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.count("element").over(w2).alias("currency_count_element"),
    )
)

scored_price_tiers = (
    scored
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .select(
        "*",
        F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w2)), 4).alias("element_dense_rank_share"),
        F.sum("element").over(w2).alias("currency_sum_element"),
    )
)

filtered = (
    scored_price_tiers
    .filter(F.col("element_dense_rank") == 3)
    .select(
        "*",
        F.ceil("element_dense_rank_share").alias("element_dense_rank_share_whole"),
    )
)

result = (
    filtered
    .groupBy("currency")
    .agg(
        F.max(F.when(F.col("currency") == "EUR", F.col("element"))).alias("eur_max_element"),
        F.sum(F.when(F.col("currency") == "USD", F.col("element_dense_rank_share_whole")).otherwise(0)).alias("usd_element_dense_rank_share_whole"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.min("element_dense_rank_share").alias("min_element_dense_rank_share"),
    )
)
