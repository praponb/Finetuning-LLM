from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.partitionBy("currency", "plan")
w3 = Window.orderBy(F.col("val").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").desc_nulls_first())
w5 = Window.orderBy("val_percent_rank")
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

with_windows = (
    lines
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .withColumn("val_percent_rank", F.percent_rank().over(w))
)

filtered = (
    with_windows
    .filter(F.col("plan").isin(["starter", "pro"]))
    .select("*", F.substring("plan", 1, 4).alias("plan_prefix"))
)

scored = (
    filtered
    .withColumn("currency_avg_val_percent_rank", F.avg("val_percent_rank").over(w2))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w3))
    .withColumn("val_percent_rank_percent_rank", F.percent_rank().over(w4))
)

enriched = (
    scored
    .select(
        "id",
        "val",
        "plan_prefix",
        "currency_avg_val_percent_rank",
        "val_percent_rank_2",
        "val_percent_rank_percent_rank",
        F.percent_rank().over(w5).alias("val_percent_rank_percent_rank_2"),
        F.avg("val_percent_rank_percent_rank").over(w6).alias("plan_avg_val_percent_rank_percent_rank"),
        F.rank().over(w7).alias("val_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "val",
        "plan_prefix",
        "val_percent_rank_2",
        "val_percent_rank_percent_rank_2",
        "plan_avg_val_percent_rank_percent_rank",
    )
)
