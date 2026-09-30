from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element_rounded").desc_nulls_first())
w2 = Window.orderBy(F.col("element_rounded").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("element"))
)

filtered = (
    lines
    .filter(F.col("effective_from") >= F.lit("2024-02-09").cast("date"))
    .select(
        "*",
        F.round("element", 2).alias("element_rounded"),
        F.add_months("effective_from", 6).alias("effective_from_plus_months"),
    )
)

enriched = (
    filtered
    .filter(F.col("effective_from_plus_months").between("2024-07-30", "2025-10-23"))
    .select(
        "id",
        "effective_from",
        "element",
        "element_rounded",
        F.rank().over(w).alias("element_rounded_rank"),
    )
)

scored = enriched.select("*", F.cume_dist().over(w2).alias("element_rounded_cume_dist"))

result = (
    scored
    .select(
        "id",
        "effective_from",
        "element",
        "element_rounded",
        "element_rounded_rank",
        "element_rounded_cume_dist",
    )
    .orderBy(F.col("element").desc(), "element_rounded")
)
