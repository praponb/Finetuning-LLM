from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "effective_from"), "currency").orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("val_rank").desc_nulls_last())
w3 = Window.partitionBy(F.date_trunc("month", "effective_from"), "plan")
w4 = Window.partitionBy("plan", "currency")
w5 = Window.orderBy("val")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

with_windows = lines.withColumn("val_rank", F.rank().over(w))

with_windows_price_tiers = (
    with_windows
    .filter(F.col("currency").isin(["THB", "USD"]))
    .select(
        "*",
        F.cume_dist().over(w2).alias("val_rank_cume_dist"),
        F.round((F.col("val_rank") / F.sum("val_rank").over(w3)), 4).alias("val_rank_share"),
    )
)

with_windows_price_tiers_2 = (
    with_windows_price_tiers
    .select(
        "plan",
        "effective_from",
        "val",
        "val_rank",
        "val_rank_share",
        F.max("val_rank_share").over(w4).alias("plan_max_val_rank_share"),
        F.percent_rank().over(w5).alias("val_percent_rank"),
    )
)

result = (
    with_windows_price_tiers_2
    .select("effective_from", "plan_max_val_rank_share", "val_percent_rank")
)
