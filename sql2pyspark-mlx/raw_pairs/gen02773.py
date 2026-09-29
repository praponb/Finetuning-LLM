from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy(F.date_trunc("month", "effective_from_minus_42d"), "currency").orderBy(F.col("val").desc())
w2 = Window.orderBy(F.col("val").desc())
w_recent_2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

filtered = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-04-09").cast("date"))
    .select(
        "*",
        F.date_sub("effective_from", 42).alias("effective_from_minus_42d"),
        F.when(F.col("currency").isin(["THB", "USD"]), "Tier 1").otherwise("Other").alias("currency_group"),
    )
)

metrics = (
    filtered
    .filter(F.col("val") > 51)
    .select(
        "id",
        "effective_from",
        "currency",
        F.rank().over(w_recent).alias("val_rank"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.percent_rank().over(w_recent_2).alias("val_percent_rank"),
    )
)

cleaned = (
    metrics
    .filter((F.col("effective_from") > F.lit("2024-05-06").cast("date")) & (F.col("currency") == "USD"))
    .withColumn("least_val_cume_dist_val_percent_rank", F.least("val_cume_dist", "val_percent_rank"))
    .withColumn("adjusted_val_cume_dist", F.round((F.col("val_cume_dist") * 0.85), 2))
)

result = (
    cleaned
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.sum(F.when(F.col("currency") == "THB", F.col("val_cume_dist")).otherwise(0)).alias("thb_val_cume_dist"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
    )
    .orderBy("thb_val_cume_dist")
)
