from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.orderBy(F.col("tier_value").desc())
w5 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w_ordered = Window.partitionBy("currency")
w_recent = Window.orderBy(F.col("tier_value_cume_dist_3").asc_nulls_first())
w8 = Window.partitionBy("currency").orderBy(F.col("tier_value_cume_dist_3").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

scored = (
    lines
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w3))
    .withColumn("tier_value_cume_dist_2", F.cume_dist().over(w4))
    .withColumn("tier_value_cume_dist_3", F.cume_dist().over(w5))
)

metrics = (
    scored
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_value",
        "tier_value_dense_rank",
        "tier_value_cume_dist_3",
        F.max("tier_value_percent_rank").over(w_ordered).alias("currency_max_tier_value_percent_rank"),
        F.percent_rank().over(w_recent).alias("tier_value_cume_dist_3_percent_rank"),
        F.rank().over(w_recent).alias("tier_value_cume_dist_3_rank"),
        F.dense_rank().over(w8).alias("tier_value_cume_dist_3_dense_rank"),
    )
)

base = (
    metrics
    .filter((F.col("effective_from") >= F.lit("2024-01-26").cast("date")) & (F.col("tier_value_cume_dist_3_rank") <= 7))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_value",
        "tier_value_dense_rank",
        "tier_value_cume_dist_3",
        "currency_max_tier_value_percent_rank",
        "tier_value_cume_dist_3_percent_rank",
        "tier_value_cume_dist_3_rank",
        "tier_value_cume_dist_3_dense_rank",
        F.round((F.col("tier_value_dense_rank") / F.col("tier_value_cume_dist_3_rank")), 2).alias("tier_value_dense_rank_per_tier_value_cume_dist_3_rank"),
    )
)

totals = (
    base
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .groupingSets(
        [["year_start", "currency"], ["year_start"], []],
        "year_start",
        "currency",
    )
    .agg(
        F.round(F.avg("tier_value_cume_dist_3_rank"), 2).alias("avg_tier_value_cume_dist_3_rank"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
)

result = (
    totals
    .select(
        "year_start",
        "avg_tier_value_cume_dist_3_rank",
        "usd_events",
        "currency_list",
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
