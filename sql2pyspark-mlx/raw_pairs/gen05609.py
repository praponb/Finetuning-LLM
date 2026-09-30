from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency").orderBy("tier_value")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value").desc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

with_windows = (
    flattened
    .withColumn("tier_value_rank", F.rank().over(w_ordered))
    .withColumn("currency_min_tier_value", F.min("tier_value").over(w2))
    .withColumn("tier_value_rank_2", F.rank().over(w3))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w4))
)

cleaned = (
    with_windows
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-05-01"))) & (F.col("currency") != "THB"))
    .withColumn("currency_title", F.initcap(F.trim("currency")))
    .withColumn("is_eur", F.when(F.col("currency") == "EUR", 1).otherwise(0))
)

metrics = (
    cleaned
    .select(
        "*",
        F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w2)), 4).alias("tier_value_rank_share"),
        F.sum("is_eur").over(w2).alias("currency_sum_is_eur"),
    )
)

base = (
    metrics
    .filter(F.col("effective_from") >= F.lit("2024-06-09").cast("date"))
    .select(
        "*",
        F.when(F.col("currency_min_tier_value") >= 84, "High").when(F.col("currency_min_tier_value") >= 39, "Medium").otherwise("Low").alias("currency_min_tier_value_band"),
        F.ceil("tier_value_rank_2").alias("tier_value_rank_2_whole"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("tier_value_rank_2") >= 7, "LARGE").otherwise("SMALL").alias("tier_value_rank_2_tier"),
        F.when(F.col("tier_value") >= 64, "HIGH").otherwise("LOW").alias("tier_value_tier"),
        F.when(F.col("tier_value_rank_share") >= 0.61, "LARGE").otherwise("SMALL").alias("tier_value_rank_share_tier"),
    )
    .agg(
        F.round(F.sum("tier_value_rank_share") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("tier_value_rank_share_per_usd"),
        F.sum(F.when(F.col("currency") == "USD", F.col("is_eur")).otherwise(0)).alias("usd_is_eur"),
        F.count("*").alias("cnt"),
        F.round(F.sum("tier_value_cume_dist") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("tier_value_cume_dist_per_usd"),
        F.sum("tier_value_rank_2_whole").alias("total_tier_value_rank_2_whole"),
    )
    .orderBy(F.col("total_tier_value_rank_2_whole").desc_nulls_last())
)
