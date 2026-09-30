from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("val").desc_nulls_last())
w_ordered = Window.orderBy(F.col("val").desc_nulls_first())
w_running = Window.orderBy(F.col("val_percent_rank").desc())
w4 = Window.orderBy(F.col("val_rank").desc_nulls_last())
w5 = Window.orderBy("val_rank")
w6 = Window.orderBy(F.col("val_percent_rank_rank").desc_nulls_first())

flattened = df_price_tiers.select("id", F.explode("tier_prices").alias("val"))

enriched = (
    flattened
    .filter(F.col("val").between(53, 75))
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("val_dense_rank"),
        F.rank().over(w_recent).alias("val_rank"),
        F.percent_rank().over(w_ordered).alias("val_percent_rank"),
        F.cume_dist().over(w_ordered).alias("val_cume_dist"),
    )
)

base = (
    enriched
    .filter(F.col("val_cume_dist") >= 0.51)
    .select(
        "*",
        F.greatest("val_dense_rank", "val").alias("greatest_val_dense_rank_val"),
        F.round((F.col("val_percent_rank") * 0.85), 2).alias("val_percent_rank_scaled"),
    )
)

filtered = (
    base
    .filter(F.col("greatest_val_dense_rank_val").between(23, 71))
    .withColumn("val_percent_rank_level", F.when(F.col("val_percent_rank") >= 0.66, "High").when(F.col("val_percent_rank") >= 0.12, "Medium").otherwise("Low"))
)

enriched_price_tiers = (
    filtered
    .select(
        "id",
        "val",
        "val_rank",
        "val_percent_rank",
        "val_cume_dist",
        "greatest_val_dense_rank_val",
        "val_percent_rank_level",
        F.rank().over(w_running).alias("val_percent_rank_rank"),
        F.percent_rank().over(w4).alias("val_rank_percent_rank"),
        F.dense_rank().over(w5).alias("val_rank_dense_rank"),
    )
)

metrics = (
    enriched_price_tiers
    .select("*", F.percent_rank().over(w6).alias("val_percent_rank_rank_percent_rank"))
)

result = (
    metrics
    .select(
        "id",
        "val",
        "val_rank",
        "val_percent_rank",
        "val_cume_dist",
        "greatest_val_dense_rank_val",
        "val_percent_rank_level",
        "val_percent_rank_rank",
        "val_rank_dense_rank",
    )
    .orderBy(F.col("id").desc())
)
