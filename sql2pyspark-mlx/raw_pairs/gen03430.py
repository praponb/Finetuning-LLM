from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("plan_max_val").desc())
w_recent = Window.partitionBy("plan").orderBy(F.col("plan_max_val").desc_nulls_last())
w5 = Window.orderBy("plan_max_val_rank")
w6 = Window.partitionBy("plan").orderBy("plan_max_val_rank")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

with_windows = (
    exploded
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .select(
        "id",
        "plan",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.max("val").over(w2).alias("plan_max_val"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w3).alias("plan_max_val_dense_rank"),
        F.sum("plan_max_val").over(w2).alias("plan_sum_plan_max_val"),
        F.rank().over(w_recent).alias("plan_max_val_rank"),
    )
)

scored_price_tiers = (
    scored
    .select(
        "plan",
        "val_dense_rank",
        "plan_sum_plan_max_val",
        "plan_max_val_rank",
        F.round((F.col("plan_max_val_rank") / F.sum("plan_max_val_rank").over(w2)), 4).alias("plan_max_val_rank_share"),
        F.rank().over(w5).alias("plan_max_val_rank_rank"),
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w2)), 4).alias("val_dense_rank_share"),
        F.dense_rank().over(w6).alias("plan_max_val_rank_dense_rank"),
    )
)

result = (
    scored_price_tiers
    .cube("plan")
    .agg(
        F.percentile_approx("plan_max_val_rank_rank", [0.25, 0.5, 0.75]).alias("plan_max_val_rank_rank_quartiles"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.percentile_approx("plan_max_val_rank_rank", 0.95).alias("plan_max_val_rank_rank_p95"),
    )
    .orderBy(F.col("row_count").desc())
)
