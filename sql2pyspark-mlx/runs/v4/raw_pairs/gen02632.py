from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy(F.col("element").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("currency_sum_element_2_plus_currency_sum_element").asc_nulls_first())
w5 = Window.orderBy(F.col("currency_sum_element_2_plus_currency_sum_element_dense_rank").asc_nulls_last())
w6 = Window.orderBy(F.col("currency_sum_element").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

metrics = (
    flattened
    .withColumn("currency_sum_element", F.sum("element").over(w))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
    .withColumn("element_percent_rank_2", F.percent_rank().over(w2))
    .withColumn("element_cume_dist", F.cume_dist().over(w3))
    .withColumn("currency_sum_element_2", F.sum("element").over(w))
)

filtered = (
    metrics
    .filter(F.col("effective_from") >= F.lit("2024-03-19").cast("date"))
    .select(
        "effective_from",
        "currency",
        "currency_sum_element",
        "element_cume_dist",
        "currency_sum_element_2",
        F.when(F.col("currency") == "THB", 1).otherwise(0).alias("is_thb"),
    )
)

filtered_price_tiers = (
    filtered
    .filter((F.col("effective_from") >= F.lit("2024-03-12").cast("date")) & F.col("currency_sum_element_2").between(207, 249))
    .withColumn("is_thb_rounded", F.round("is_thb", 0))
    .withColumn("currency_sum_element_2_plus_currency_sum_element", (F.col("currency_sum_element_2") + F.col("currency_sum_element")))
)

metrics_price_tiers = (
    filtered_price_tiers
    .withColumn("currency_sum_element_2_plus_currency_sum_element_dense_rank", F.dense_rank().over(w4))
    .withColumn("currency_count_currency_sum_element", F.count("currency_sum_element").over(w))
)

scored = (
    metrics_price_tiers
    .withColumn("currency_sum_element_2_plus_currency_sum_element_dense_rank_dense_rank", F.dense_rank().over(w5))
    .withColumn("currency_sum_element_percent_rank", F.percent_rank().over(w6))
)

result = (
    scored
    .select(
        "currency",
        "currency_sum_element_percent_rank",
        "is_thb_rounded",
        "currency_sum_element_2_plus_currency_sum_element",
        "element_cume_dist",
    )
    .unpivot(
        ["currency"],
        ["currency_sum_element_percent_rank", "is_thb_rounded", "currency_sum_element_2_plus_currency_sum_element", "element_cume_dist"],
        "metric_name",
        "metric_value",
    )
)
