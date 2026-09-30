from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("plan", "currency")
w3 = Window.partitionBy("currency")
w_running = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_part = Window.partitionBy("plan")
w_ordered = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w7 = Window.partitionBy("currency").orderBy(F.col("tier_value_dense_rank_2").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.count("tier_value").over(w2).alias("plan_count_tier_value"),
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.stddev_samp("tier_value").over(w3).alias("currency_stddev_samp_tier_value"),
    )
)

with_windows = (
    enriched
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w_running))
    .withColumn("plan_avg_tier_value_dense_rank", F.avg("tier_value_dense_rank").over(w_part))
)

with_windows_price_tiers = (
    with_windows
    .withColumn("plan_avg_tier_value", F.avg("tier_value").over(w2))
    .withColumn("tier_value_dense_rank_3", F.dense_rank().over(w_ordered))
    .withColumn("tier_value_dense_rank_2_dense_rank", F.dense_rank().over(w7))
)

result = (
    with_windows_price_tiers
    .groupBy(
        "currency",
        F.when(F.col("tier_value_cume_dist") >= 0.71, "HIGH").otherwise("LOW").alias("tier_value_cume_dist_tier"),
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("tier_value"))).alias("eur_total_tier_value"),
        F.sum(F.when(F.col("currency") == "THB", F.col("plan_count_tier_value")).otherwise(0)).alias("thb_plan_count_tier_value"),
    )
)
