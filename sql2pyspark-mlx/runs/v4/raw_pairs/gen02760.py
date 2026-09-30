from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w3 = Window.orderBy(F.col("val_dense_rank").asc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("val_rank").desc())
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("currency")
w7 = Window.orderBy(F.col("val_dense_rank_percent_rank").asc_nulls_last())
w8 = Window.partitionBy("plan").orderBy(F.col("val_dense_rank_percent_rank").asc_nulls_last())
w9 = Window.partitionBy("currency", "plan")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("val"))
)

with_windows = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.rank().over(w2).alias("val_rank"),
    )
)

enriched = (
    with_windows
    .withColumn("val_dense_rank_percent_rank", F.percent_rank().over(w3))
    .withColumn("val_rank_rank", F.rank().over(w4))
    .withColumn("plan_avg_val", F.avg("val").over(w5))
    .withColumn("val_dense_rank_share", F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w6)), 4))
)

scored = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w7).alias("val_dense_rank_percent_rank_percent_rank"),
        F.cume_dist().over(w8).alias("val_dense_rank_percent_rank_cume_dist"),
        F.sum("plan_avg_val").over(w9).alias("currency_sum_plan_avg_val"),
    )
)

cleaned = (
    scored
    .filter(F.col("val_rank_rank").between(3, 5))
    .withColumn("currency_title", F.initcap(F.trim("currency")))
)

filtered = (
    cleaned
    .filter(F.col("currency_sum_plan_avg_val") == 346)
    .withColumn("val_dense_rank_percent_rank_cume_dist_minus_currency_sum_plan_avg_val", (F.col("val_dense_rank_percent_rank_cume_dist") - F.col("currency_sum_plan_avg_val")))
)

result = (
    filtered
    .select(
        "plan",
        "val",
        "val_dense_rank",
        "val_dense_rank_percent_rank",
        "val_rank_rank",
        "plan_avg_val",
        "val_dense_rank_share",
        "val_dense_rank_percent_rank_percent_rank",
        "val_dense_rank_percent_rank_cume_dist",
    )
)
