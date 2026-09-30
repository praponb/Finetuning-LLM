from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.partitionBy("currency").orderBy("element")
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("currency", "plan")
w5 = Window.partitionBy("currency").orderBy("element_rank")
w6 = Window.orderBy(F.col("element_percent_rank").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

metrics = (
    flattened
    .withColumn("element_rank", F.rank().over(w))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
    .withColumn("plan_min_element", F.min("element").over(w3))
    .withColumn("currency_max_element", F.max("element").over(w4))
)

with_windows = (
    metrics
    .withColumn("element_rank_rank", F.rank().over(w5))
    .withColumn("element_percent_rank_cume_dist", F.cume_dist().over(w6))
)

filtered = (
    with_windows
    .filter(F.col("effective_from") >= F.lit("2024-07-19").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "element_rank",
        "element_percent_rank",
        "plan_min_element",
        "currency_max_element",
        "element_rank_rank",
        "element_percent_rank_cume_dist",
        F.col("element_rank").cast("string").alias("element_rank_str"),
    )
)

cleaned = (
    filtered
    .filter(F.col("element_percent_rank").isNotNull())
    .select(
        "currency",
        "element_percent_rank",
        "plan_min_element",
        "currency_max_element",
        "element_percent_rank_cume_dist",
        "element_rank_str",
        F.when(F.col("plan") == "enterprise", 1).otherwise(0).alias("is_enterprise"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("element_percent_rank") >= 0.89, "LARGE").otherwise("SMALL").alias("element_percent_rank_tier"),
        "currency",
    )
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("element_percent_rank_cume_dist")).otherwise(0)).alias("eur_element_percent_rank_cume_dist"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
    .filter(F.col("eur_events") > 1)
)
