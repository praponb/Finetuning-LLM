from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").asc_nulls_last())
w2 = Window.orderBy(F.col("element_filled").asc_nulls_first())
w3 = Window.orderBy(F.col("element_filled_percent_rank").desc_nulls_first())
w4 = Window.orderBy(F.col("element_filled_percent_rank").desc())

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

base = (
    flattened
    .filter(F.col("currency").isin(["EUR", "USD"]))
    .select(
        "effective_from",
        "element",
        F.concat("currency", F.lit(", "), "plan").alias("currency_plan"),
    )
)

with_windows = (
    base
    .select(
        "effective_from",
        "element",
        "currency_plan",
        F.percent_rank().over(w).alias("element_percent_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("element_percent_rank") >= 0.56)
    .select(
        "*",
        F.coalesce("element", F.lit(0)).alias("element_filled"),
        (F.col("element_percent_rank") + F.col("element")).alias("element_percent_rank_plus_element"),
    )
)

metrics = (
    cleaned
    .select(
        "effective_from",
        "currency_plan",
        "element_filled",
        F.percent_rank().over(w2).alias("element_filled_percent_rank"),
    )
)

scored = (
    metrics
    .select(
        "currency_plan",
        "element_filled",
        F.percent_rank().over(w3).alias("element_filled_percent_rank_percent_rank"),
        F.dense_rank().over(w4).alias("element_filled_percent_rank_dense_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("element_filled").between(51, 67))
    .withColumn("element_filled_percent_rank_dense_rank_plus_element_filled_percent_rank_percent_rank", (F.col("element_filled_percent_rank_dense_rank") + F.col("element_filled_percent_rank_percent_rank")))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("element_filled_percent_rank_percent_rank") >= 0.86, "PREMIUM").otherwise("STANDARD").alias("element_filled_percent_rank_percent_rank_tier"),
    )
    .agg(
        F.max("element_filled").alias("max_element_filled"),
        F.percentile_approx("element_filled", [0.25, 0.5, 0.75]).alias("element_filled_quartiles"),
        F.count("*").alias("row_count"),
    )
)
