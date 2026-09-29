from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

scored = (
    exploded
    .withColumn("currency_sum_val", F.sum("val").over(w))
    .withColumn("currency_min_val", F.min("val").over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
)

filtered = (
    scored
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-06-02")))
    .select(
        "id",
        "currency",
        "currency_sum_val",
        "currency_min_val",
        F.coalesce("val_percent_rank", F.lit(0)).alias("val_percent_rank_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.avg("currency_min_val").over(w).alias("currency_avg_currency_min_val"),
        F.sum("currency_sum_val").over(w).alias("currency_sum_currency_sum_val"),
    )
)

cleaned = (
    enriched
    .filter(F.col("currency_sum_val") < 112)
    .select(
        "id",
        "currency",
        "currency_min_val",
        "val_percent_rank_filled",
        "currency_avg_currency_min_val",
        "currency_sum_currency_sum_val",
        (F.col("currency_sum_currency_sum_val") - F.col("currency_avg_currency_min_val")).alias("currency_sum_currency_sum_val_minus_currency_avg_currency_min_val"),
    )
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("currency_sum_currency_sum_val_minus_currency_avg_currency_min_val") < 39)
    .select(
        "id",
        "currency",
        "val_percent_rank_filled",
        "currency_avg_currency_min_val",
        "currency_sum_currency_sum_val",
        F.col("currency_avg_currency_min_val").cast("string").alias("currency_avg_currency_min_val_num"),
    )
)

result = (
    cleaned_price_tiers
    .groupBy(
        F.when(F.col("currency_avg_currency_min_val") >= 69, "LARGE").otherwise("SMALL").alias("currency_avg_currency_min_val_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.sum("currency_sum_currency_sum_val").alias("total_currency_sum_currency_sum_val"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
)
