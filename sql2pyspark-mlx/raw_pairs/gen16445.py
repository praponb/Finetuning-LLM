from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy(F.year("effective_from"))
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("element_filled").asc_nulls_first())
w_part = Window.partitionBy(F.year("effective_from"), "currency").orderBy(F.col("element_filled").desc_nulls_first())
w_hist = Window.partitionBy("currency").orderBy(F.col("element_filled").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

cleaned = (
    lines
    .filter(F.col("effective_from") >= "2023-04-01")
    .withColumn("clean_currency", F.trim("currency"))
)

valid = (
    cleaned
    .filter(F.col("element").isNotNull())
    .select(
        "effective_from",
        "currency",
        "clean_currency",
        F.coalesce("element", F.lit(0)).alias("element_filled"),
    )
)

enriched = (
    valid
    .filter(F.month("effective_from").isin([5, 7, 12]))
    .select(
        "effective_from",
        "clean_currency",
        F.max("element_filled").over(w_running).alias("effective_from_year_max_element_filled"),
        F.count("element_filled").over(w2).alias("currency_count_element_filled"),
        F.rank().over(w3).alias("element_filled_rank"),
        F.dense_rank().over(w_part).alias("element_filled_dense_rank"),
        F.percent_rank().over(w_hist).alias("element_filled_percent_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("element_filled_dense_rank") >= 6, "PREMIUM").otherwise("STANDARD").alias("element_filled_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("element_filled_percent_rank"), 2).alias("avg_element_filled_percent_rank"),
        F.round(F.avg("effective_from_year_max_element_filled"), 2).alias("avg_effective_from_year_max_element_filled"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("element_filled_dense_rank_tier").desc())
)
