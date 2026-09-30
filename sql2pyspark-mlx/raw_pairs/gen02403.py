from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc())
w2 = Window.partitionBy(F.date_trunc("week", "effective_from")).orderBy(F.col("element").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w4 = Window.partitionBy(F.year("effective_from"), "currency").orderBy(F.col("element").desc())
w5 = Window.partitionBy(F.month("effective_from"), "currency").orderBy(F.col("element").desc_nulls_last())
w6 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

cleaned = (
    exploded
    .filter(F.month("effective_from").isin([7, 9, 10]))
    .withColumn("currency_upper", F.upper("currency"))
)

with_windows = (
    cleaned
    .select(
        "effective_from",
        "currency",
        "element",
        "currency_upper",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.percent_rank().over(w2).alias("element_percent_rank_2"),
        F.cume_dist().over(w3).alias("element_cume_dist"),
        F.dense_rank().over(w4).alias("element_dense_rank"),
    )
)

enriched = (
    with_windows
    .filter(F.col("effective_from") < "2024-11-01")
    .select(
        "effective_from",
        "currency",
        "element",
        "element_percent_rank",
        "element_percent_rank_2",
        F.percent_rank().over(w5).alias("element_percent_rank_3"),
        F.max("element_dense_rank").over(w6).alias("currency_max_element_dense_rank"),
    )
)

daily = (
    enriched
    .groupBy("currency")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.sort_array(F.collect_set("currency")).alias("currency_list_2"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("element_per_usd"),
        F.count("*").alias("row_count"),
        F.round(F.avg("element_percent_rank_2"), 2).alias("avg_element_percent_rank_2"),
    )
)

result = (
    daily
    .select(
        "currency",
        "thb_rows",
        "element_per_usd",
        "row_count",
        "avg_element_percent_rank_2",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "OTHER_ZONE").when(F.col("currency").rlike("^(USD)$"), "GROUP_A").otherwise("PRIORITY").alias("currency_zone"),
    )
    .orderBy(F.col("thb_rows").desc())
)
