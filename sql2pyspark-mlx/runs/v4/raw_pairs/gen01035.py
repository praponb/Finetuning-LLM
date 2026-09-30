from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("element")
w3 = Window.orderBy(F.col("element").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w5 = Window.partitionBy("currency")
w6 = Window.orderBy(F.col("element_percent_rank_2_times_element_percent_rank").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

with_windows = (
    flattened
    .withColumn("plan_count_element", F.count("element").over(w))
    .withColumn("element_rank", F.rank().over(w2))
    .withColumn("element_percent_rank", F.percent_rank().over(w3))
    .withColumn("element_percent_rank_2", F.percent_rank().over(w4))
    .withColumn("currency_sum_element", F.sum("element").over(w5))
)

cleaned = (
    with_windows
    .filter(F.col("element_percent_rank_2") <= 0.29)
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        "plan_count_element",
        "element_rank",
        "element_percent_rank",
        "currency_sum_element",
        F.when(F.col("element_percent_rank") >= 0.78, "High").when(F.col("element_percent_rank") >= 0.26, "Medium").otherwise("Low").alias("element_percent_rank_band"),
        (F.col("element_percent_rank_2") * F.col("element_percent_rank")).alias("element_percent_rank_2_times_element_percent_rank"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.rank().over(w6).alias("element_percent_rank_2_times_element_percent_rank_rank"),
        F.percent_rank().over(w3).alias("element_percent_rank_2"),
    )
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("currency_sum_element") >= 324, "HIGH").otherwise("LOW").alias("currency_sum_element_tier"),
        F.when(F.col("element_percent_rank_2_times_element_percent_rank") >= 79, "LARGE").otherwise("SMALL").alias("element_percent_rank_2_times_element_percent_rank_tier"),
    )
    .agg(
        F.round(F.sum("element_percent_rank_2_times_element_percent_rank") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("element_percent_rank_2_times_element_percent_rank_per_usd"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.sum(F.when(F.col("currency") == "USD", F.col("currency_sum_element")).otherwise(0)).alias("usd_currency_sum_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("element_per_thb"),
        F.avg(F.when(F.col("currency") == "USD", F.col("currency_sum_element"))).alias("usd_avg_currency_sum_element"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
    )
    .filter(F.col("usd_rows") < 1)
)

result = (
    grouped
    .select(
        "currency_sum_element_tier",
        "element_percent_rank_2_times_element_percent_rank_tier",
        "element_percent_rank_2_times_element_percent_rank_per_usd",
        "usd_rows",
        "usd_currency_sum_element",
        "element_per_thb",
        "usd_avg_currency_sum_element",
        "thb_rows",
    )
    .orderBy(F.col("thb_rows").desc())
)
