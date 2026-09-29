from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "effective_from"), "currency").orderBy(F.col("val").desc_nulls_first())
w_recent = Window.partitionBy("currency").orderBy("val")
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("val").desc())
w5 = Window.partitionBy(F.year("effective_from"), "currency").orderBy(F.col("val_dense_rank_2").desc_nulls_first())
w6 = Window.orderBy(F.col("val_rank").desc())
w7 = Window.orderBy(F.col("val_rank_percent_rank").desc_nulls_last())
w8 = Window.orderBy("val_rank_percent_rank_dense_rank")

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

with_windows = (
    lines
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("val_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("val_rank_2", F.rank().over(w3))
    .withColumn("val_dense_rank_2", F.dense_rank().over(w4))
)

enriched = (
    with_windows
    .filter(F.col("val_rank_2").between(4, 8))
    .select(
        "id",
        "effective_from",
        "val_rank",
        "val_percent_rank",
        "val_dense_rank_2",
        F.cume_dist().over(w5).alias("val_dense_rank_2_cume_dist"),
        F.percent_rank().over(w6).alias("val_rank_percent_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("val_rank") == 4)
    .select(
        "*",
        (F.col("val_dense_rank_2") / F.nullif("val_rank_percent_rank", F.lit(0))).alias("val_dense_rank_2_per_val_rank_percent_rank"),
    )
)

filtered = (
    cleaned
    .filter(F.col("effective_from") >= F.lit("2024-05-19").cast("date"))
    .select(
        "*",
        F.col("val_dense_rank_2_per_val_rank_percent_rank").cast("int").alias("val_dense_rank_2_per_val_rank_percent_rank_int"),
    )
)

enriched_price_tiers = filtered.withColumn("val_rank_percent_rank_dense_rank", F.dense_rank().over(w7))

scored = (
    enriched_price_tiers
    .select(
        "id",
        "effective_from",
        "val_rank",
        "val_dense_rank_2_cume_dist",
        "val_dense_rank_2_per_val_rank_percent_rank",
        F.rank().over(w8).alias("val_rank_percent_rank_dense_rank_rank"),
        F.cume_dist().over(w8).alias("val_rank_percent_rank_dense_rank_cume_dist"),
    )
)

result = (
    scored
    .select(
        "effective_from",
        "val_rank",
        "val_dense_rank_2_per_val_rank_percent_rank",
        "val_rank_percent_rank_dense_rank_rank",
        "val_rank_percent_rank_dense_rank_cume_dist",
    )
    .orderBy(F.col("val_rank_percent_rank_dense_rank_cume_dist").asc_nulls_first())
)
