from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc())
w_recent = Window.orderBy(F.col("val_dense_rank").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

base = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-03-08").cast("date"))
    .withColumn("clean_plan", F.trim("plan"))
    .withColumn("val_filled", F.coalesce("val", F.lit(0)))
)

scored = (
    base
    .filter(F.col("effective_from") > "2024-03-12")
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        F.dense_rank().over(w).alias("val_dense_rank"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "currency",
        "val",
        F.percent_rank().over(w_recent).alias("val_dense_rank_percent_rank"),
    )
)

result = (
    metrics
    .select("val", "val_dense_rank_percent_rank")
    .orderBy(F.col("val").desc_nulls_last(), "val_dense_rank_percent_rank")
)
