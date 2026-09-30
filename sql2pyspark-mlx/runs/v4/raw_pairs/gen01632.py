from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("tier_value")
w2 = Window.orderBy(F.col("tier_value").desc())
w3 = Window.orderBy(F.col("tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("tier_value"))
)

metrics = (
    lines
    .select(
        "*",
        F.dense_rank().over(w_running).alias("tier_value_dense_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.dense_rank().over(w_running).alias("tier_value_dense_rank_2"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank_3"),
    )
)

valid = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-07-06").cast("date"))
    .select(
        "*",
        (F.col("tier_value_dense_rank") * F.col("tier_value")).alias("tier_value_dense_rank_times_tier_value"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("tier_value_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("tier_value_dense_rank_tier"),
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
    )
    .agg(
        F.avg("tier_value_cume_dist").alias("avg_tier_value_cume_dist"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
