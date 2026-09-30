from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("val_filled").desc_nulls_first())
w4 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("plan_sum_val_cume_dist").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

filtered = (
    exploded
    .filter(F.col("effective_from").between("2023-07-01", "2025-05-01"))
    .withColumn("val_filled", F.coalesce("val", F.lit(0)))
)

scored = (
    filtered
    .filter(F.col("plan").isin(["basic", "starter", "pro"]))
    .select("*", F.cume_dist().over(w).alias("val_cume_dist"))
)

with_windows = (
    scored
    .select(
        "id",
        "plan",
        "effective_from",
        "val_filled",
        "val_cume_dist",
        F.avg("val_filled").over(w2).alias("plan_avg_val_filled"),
        F.rank().over(w3).alias("val_filled_rank"),
        F.sum("val_cume_dist").over(w2).alias("plan_sum_val_cume_dist"),
    )
)

metrics = (
    with_windows
    .withColumn("plan_sum_val_cume_dist_rank", F.rank().over(w4))
    .withColumn("plan_sum_val_cume_dist_2", F.sum("val_cume_dist").over(w2))
    .withColumn("plan_count_val_cume_dist", F.count("val_cume_dist").over(w2))
)

base = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-02-19").cast("date"))
    .withColumn("least_plan_sum_val_cume_dist_rank_val_filled_rank", F.least("plan_sum_val_cume_dist_rank", "val_filled_rank"))
)

filtered_price_tiers = (
    base
    .filter(F.col("least_plan_sum_val_cume_dist_rank_val_filled_rank").between(73, 76))
    .select(
        "*",
        F.least("val_cume_dist", "plan_count_val_cume_dist").alias("least_val_cume_dist_plan_count_val_cume_dist"),
    )
)

result = (
    filtered_price_tiers
    .select(
        "id",
        "plan",
        "val_cume_dist",
        "plan_avg_val_filled",
        "val_filled_rank",
        "plan_sum_val_cume_dist",
        "plan_sum_val_cume_dist_rank",
        "plan_sum_val_cume_dist_2",
        "least_plan_sum_val_cume_dist_rank_val_filled_rank",
    )
)
