from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy("currency")

exploded = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("val"))

filtered = (
    exploded
    .filter(F.col("currency") == "THB")
    .select("*", F.initcap(F.trim("currency")).alias("currency_title"))
)

with_windows = (
    filtered
    .filter(F.col("currency") == "THB")
    .withColumn("val_cume_dist", F.cume_dist().over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
)

metrics = (
    with_windows
    .select(
        "currency",
        "val",
        "currency_title",
        "val_percent_rank",
        F.min("val").over(w3).alias("currency_min_val"),
    )
)

result = (
    metrics
    .groupBy("currency")
    .agg(
        F.avg("currency_min_val").alias("avg_currency_min_val"),
        F.round(F.sum("currency_min_val") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("currency_min_val_per_eur"),
        F.percentile_approx("val_percent_rank", 0.9).alias("val_percent_rank_p90"),
    )
    .orderBy("currency_min_val_per_eur")
)
