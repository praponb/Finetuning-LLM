from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("val").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

metrics = (
    flattened
    .filter(F.col("val") < 53)
    .select(
        "id",
        "plan",
        F.min("val").over(w).alias("plan_min_val"),
        F.count("val").over(w2).alias("plan_count_val"),
        F.percent_rank().over(w3).alias("val_percent_rank"),
        F.dense_rank().over(w4).alias("val_dense_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("plan_count_val") != 12)
    .withColumn("val_percent_rank_filled", F.coalesce("val_percent_rank", F.lit(0)))
)

metrics_price_tiers = (
    cleaned
    .select(
        "plan",
        "plan_min_val",
        "plan_count_val",
        "val_percent_rank",
        "val_dense_rank",
        "val_percent_rank_filled",
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w2)), 4).alias("val_dense_rank_share"),
    )
)

valid = (
    metrics_price_tiers
    .filter(F.col("plan_count_val").between(10, 12))
    .withColumn("val_percent_rank_filled_2", F.coalesce("val_percent_rank", F.lit(0)))
)

result = (
    valid
    .select(
        "plan_min_val",
        "plan_count_val",
        "val_percent_rank",
        "val_percent_rank_filled",
        "val_dense_rank_share",
    )
    .orderBy(F.col("val_percent_rank_filled").desc_nulls_first())
)
