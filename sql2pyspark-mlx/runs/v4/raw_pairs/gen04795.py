from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w_running = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.orderBy(F.col("tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.max("tier_value").over(w_ordered).alias("plan_max_tier_value"),
        F.min("tier_value").over(w_ordered).alias("plan_min_tier_value"),
        F.dense_rank().over(w_running).alias("tier_value_dense_rank"),
        F.avg("tier_value").over(w_ordered).alias("plan_avg_tier_value"),
    )
)

metrics = (
    enriched
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w3))
    .withColumn("plan_avg_plan_min_tier_value", F.avg("plan_min_tier_value").over(w_ordered))
)

valid = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-07-06").cast("date"))
    .withColumn("tier_value_cume_dist_whole", F.ceil("tier_value_cume_dist"))
    .withColumn("tier_value_dense_rank_per_plan_avg_tier_value", (F.col("tier_value_dense_rank") / F.nullif("plan_avg_tier_value", F.lit(0))))
)

with_windows = (
    valid
    .withColumn("plan_avg_tier_value_cume_dist_whole", F.avg("tier_value_cume_dist_whole").over(w_ordered))
    .withColumn("plan_avg_plan_min_tier_value_2", F.avg("plan_min_tier_value").over(w_ordered))
)

result = (
    with_windows
    .cube(
        F.when(F.col("plan_min_tier_value") >= 79, "HIGH").otherwise("LOW").alias("plan_min_tier_value_tier"),
        "plan",
    )
    .agg(
        F.percentile_approx("plan_avg_tier_value_cume_dist_whole", [0.25, 0.5, 0.75]).alias("plan_avg_tier_value_cume_dist_whole_quartiles"),
        F.max("tier_value_cume_dist_whole").alias("max_tier_value_cume_dist_whole"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.percentile_approx("plan_avg_plan_min_tier_value", [0.25, 0.5, 0.75]).alias("plan_avg_plan_min_tier_value_quartiles"),
    )
)
