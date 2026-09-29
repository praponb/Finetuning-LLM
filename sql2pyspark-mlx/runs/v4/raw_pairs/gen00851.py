from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("element").desc())
w2 = Window.orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy(F.col("element_rank_2").asc_nulls_last())
w_ordered = Window.orderBy(F.col("element_rank").desc())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

cleaned = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-09-29").cast("date"))
    .withColumn("effective_from_plus_months", F.add_months("effective_from", 12))
)

enriched = (
    cleaned
    .withColumn("element_rank", F.rank().over(w_running))
    .withColumn("element_rank_2", F.rank().over(w2))
)

with_windows = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w3).alias("element_rank_2_cume_dist"),
        F.rank().over(w_running).alias("element_rank_3"),
        F.cume_dist().over(w_ordered).alias("element_rank_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "effective_from",
        "element",
        "effective_from_plus_months",
        "element_rank",
        "element_rank_2_cume_dist",
    )
)
