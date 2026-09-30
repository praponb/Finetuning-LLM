from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy(F.year("effective_from")).orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.orderBy(F.col("tier_value_cume_dist_2").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value_cume_dist").asc_nulls_first())
w5 = Window.partitionBy("activity_month").orderBy(F.col("avg_tier_value_cume_dist_2_percent_rank").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("tier_value"))
)

metrics = (
    lines
    .filter(F.year("effective_from") == 2023)
    .select(
        "id",
        "effective_from",
        F.cume_dist().over(w_recent).alias("tier_value_cume_dist"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist_2"),
    )
)

filtered = (
    metrics
    .filter((F.col("effective_from") > F.lit("2024-06-09").cast("date")) & (F.col("effective_from") >= "2024-06-22"))
    .select(
        "*",
        (F.col("tier_value_cume_dist") / F.nullif("tier_value_cume_dist_2", F.lit(0))).alias("tier_value_cume_dist_per_tier_value_cume_dist_2"),
    )
)

cleaned = (
    filtered
    .filter(F.col("effective_from") >= F.lit("2024-03-14").cast("date"))
    .select(
        "id",
        "effective_from",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        F.last_day("effective_from").alias("effective_from_month_end"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w3).alias("tier_value_cume_dist_2_percent_rank"),
        F.dense_rank().over(w4).alias("tier_value_cume_dist_dense_rank"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.year("effective_from") >= 2023)
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("MONTH", "effective_from_month_end").alias("activity_month"),
        F.when(F.col("tier_value_cume_dist_2_percent_rank") >= 0.75, "HIGH").otherwise("LOW").alias("tier_value_cume_dist_2_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("tier_value_cume_dist_2_percent_rank"), 2).alias("avg_tier_value_cume_dist_2_percent_rank"),
        F.avg("tier_value_cume_dist_2").alias("avg_tier_value_cume_dist_2"),
    )
    .filter(F.col("avg_tier_value_cume_dist_2_percent_rank") < 1000000)
    .withColumn("avg_tier_value_cume_dist_2_percent_rank_dense_rank", F.dense_rank().over(w5))
)
