from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())

lines = df_price_tiers.select("id", F.explode("tier_prices").alias("element"))

with_windows = (
    lines
    .filter(F.col("element") <= 31)
    .select("id", "element", F.dense_rank().over(w).alias("element_dense_rank"))
)

base = (
    with_windows
    .filter(F.col("element").between(30, 78))
    .withColumn("element_whole", F.ceil("element"))
)

base_price_tiers = (
    base
    .filter(F.col("element_whole") > 37)
    .withColumn("least_element_element_dense_rank", F.least("element", "element_dense_rank"))
)

filtered = (
    base_price_tiers
    .filter(F.col("element") >= 71)
    .select(
        "id",
        "element",
        "element_dense_rank",
        "element_whole",
        F.col("element_whole").cast("string").alias("element_whole_str"),
        (F.col("element_dense_rank") / F.nullif("element_whole", F.lit(0))).alias("element_dense_rank_per_element_whole"),
    )
)

valid = (
    filtered
    .filter(F.col("element") < 70)
    .withColumn("element_minus_element_dense_rank_per_element_whole", (F.col("element") - F.col("element_dense_rank_per_element_whole")))
)

grouped = (
    valid
    .groupBy(
        F.when(F.col("element") >= 70, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.percentile_approx("element_minus_element_dense_rank_per_element_whole", 0.9).alias("element_minus_element_dense_rank_per_element_whole_p90"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("element_dense_rank_per_element_whole", 0.5).alias("element_dense_rank_per_element_whole_p50"),
    )
    .filter(F.col("element_minus_element_dense_rank_per_element_whole_p90") > 0)
)

result = (
    grouped
    .select(
        "element_tier",
        "element_minus_element_dense_rank_per_element_whole_p90",
        "num_rows",
        "element_dense_rank_per_element_whole_p50",
    )
    .orderBy(F.col("element_dense_rank_per_element_whole_p50").desc())
)
