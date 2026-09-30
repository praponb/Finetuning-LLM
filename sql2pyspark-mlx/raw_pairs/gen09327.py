from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("val").desc())
w4 = Window.orderBy(F.col("plan_max_val").desc_nulls_first())
w5 = Window.orderBy(F.col("val_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("val_cume_dist").asc_nulls_first())
w7 = Window.orderBy(F.col("plan_max_val_percent_rank").desc())
w8 = Window.orderBy(F.col("plan_max_val_percent_rank").desc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

metrics = (
    flattened
    .withColumn("plan_max_val", F.max("val").over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
    .withColumn("val_rank", F.rank().over(w3))
)

filtered = (
    metrics
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-03-07"))) & (F.col("plan_max_val") == 62))
    .select(
        "id",
        "plan_max_val",
        "val_cume_dist",
        "val_rank",
        F.last_day("effective_from").alias("effective_from_month_end"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "plan_max_val",
        "val_cume_dist",
        F.percent_rank().over(w4).alias("plan_max_val_percent_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.rank().over(w5).alias("val_cume_dist_rank"),
        F.percent_rank().over(w6).alias("val_cume_dist_percent_rank"),
        F.rank().over(w7).alias("plan_max_val_percent_rank_rank"),
        F.dense_rank().over(w8).alias("plan_max_val_percent_rank_dense_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("val_cume_dist_rank") > 7)
    .withColumn("val_cume_dist_percent_rank_per_plan_max_val_percent_rank", (F.col("val_cume_dist_percent_rank") / F.nullif("plan_max_val_percent_rank", F.lit(0))))
)

totals = (
    base
    .groupBy(
        F.when(F.col("plan_max_val_percent_rank_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("plan_max_val_percent_rank_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("plan_max_val", [0.25, 0.5, 0.75]).alias("plan_max_val_quartiles"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .select("plan_max_val_percent_rank_dense_rank_tier", "row_count", "distinct_ids")
)
