from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element_filled").asc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("element_filled").desc())
w3 = Window.orderBy(F.col("element").asc_nulls_first())
w4 = Window.partitionBy("currency")
w5 = Window.partitionBy("currency").orderBy(F.col("element").desc())
w6 = Window.orderBy(F.col("element").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

base = (
    flattened
    .filter(F.col("element") <= 39)
    .select("*", F.coalesce("element", F.lit(0)).alias("element_filled"))
)

enriched = (
    base
    .select(
        "id",
        "element",
        F.dense_rank().over(w).alias("element_filled_dense_rank"),
        F.percent_rank().over(w2).alias("element_filled_percent_rank"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
        F.max("element").over(w4).alias("currency_max_element"),
        F.percent_rank().over(w5).alias("element_percent_rank_2"),
    )
)

metrics = (
    enriched
    .filter(F.col("element_filled_percent_rank") > 0.49)
    .select(
        "id",
        "element_filled_dense_rank",
        "element_filled_percent_rank",
        "currency_max_element",
        "element_percent_rank_2",
        F.percent_rank().over(w6).alias("element_percent_rank_3"),
    )
)

filtered = (
    metrics
    .filter(F.col("currency_max_element") >= 61)
    .select(
        "element_filled_dense_rank",
        "element_filled_percent_rank",
        "currency_max_element",
        (F.col("currency_max_element") * F.col("element_filled_dense_rank")).alias("currency_max_element_times_element_filled_dense_rank"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("currency_max_element_times_element_filled_dense_rank") >= 67, "HIGH").otherwise("LOW").alias("currency_max_element_times_element_filled_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("element_filled_dense_rank", 0.9).alias("element_filled_dense_rank_p90"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("currency_max_element_times_element_filled_dense_rank_tier")
)
