from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("years_since_effective_from").desc_nulls_last())
w_part = Window.orderBy(F.col("element").desc_nulls_first())
w_hist = Window.partitionBy(F.month("effective_from")).orderBy(F.col("element").desc())
w4 = Window.orderBy(F.col("years_since_effective_from").desc())
w5 = Window.partitionBy(F.month("effective_from")).orderBy("years_since_effective_from_cume_dist")

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("element"))
)

cleaned = (
    exploded
    .filter(F.col("element") <= 63)
    .withColumn("years_since_effective_from", (F.datediff(F.current_date(), "effective_from") / 365))
)

enriched = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("years_since_effective_from_cume_dist"),
        F.dense_rank().over(w_part).alias("element_dense_rank"),
        F.dense_rank().over(w_part).alias("element_dense_rank_2"),
        F.cume_dist().over(w_hist).alias("element_cume_dist"),
        F.rank().over(w_recent).alias("years_since_effective_from_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("effective_from") >= F.lit("2024-02-28").cast("date"))
    .select(
        "*",
        F.least("years_since_effective_from_cume_dist", "element_dense_rank").alias("least_years_since_effective_from_cume_dist_element_dense_rank"),
    )
)

scored = (
    filtered
    .select(
        "effective_from",
        "years_since_effective_from",
        "years_since_effective_from_cume_dist",
        "element_dense_rank",
        "element_dense_rank_2",
        "element_cume_dist",
        "years_since_effective_from_rank",
        F.cume_dist().over(w4).alias("years_since_effective_from_cume_dist_2"),
    )
)

enriched_price_tiers = (
    scored
    .select("*", F.rank().over(w5).alias("years_since_effective_from_cume_dist_rank"))
)

result = (
    enriched_price_tiers
    .groupBy(
        F.when(F.col("years_since_effective_from_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("years_since_effective_from_rank_tier"),
    )
    .agg(
        F.max("years_since_effective_from_cume_dist").alias("max_years_since_effective_from_cume_dist"),
        F.percentile_approx("years_since_effective_from_cume_dist_rank", [0.25, 0.5, 0.75]).alias("years_since_effective_from_cume_dist_rank_quartiles"),
        F.percentile_approx("years_since_effective_from_cume_dist_2", [0.25, 0.5, 0.75]).alias("years_since_effective_from_cume_dist_2_quartiles"),
        F.percentile_approx("years_since_effective_from_cume_dist", 0.95).alias("years_since_effective_from_cume_dist_p95"),
        F.min("years_since_effective_from_cume_dist").alias("min_years_since_effective_from_cume_dist"),
        F.count("*").alias("event_count"),
    )
)
