from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy(F.col("element_rounded").asc_nulls_first())
w2 = Window.orderBy(F.col("element_rounded_percent_rank").desc_nulls_first())
w3 = Window.orderBy(F.col("element_rounded").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

base = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-05-11").cast("date"))
    .select(
        "id",
        "plan",
        "currency",
        "element",
        F.round("element", 0).alias("element_rounded"),
    )
)

scored = (
    base
    .select(
        "id",
        "element",
        "element_rounded",
        F.percent_rank().over(w).alias("element_rounded_percent_rank"),
    )
)

metrics = (
    scored
    .filter(F.col("element_rounded_percent_rank").between(0.4, 0.56))
    .withColumn("element_rounded_percent_rank_percent_rank", F.percent_rank().over(w2))
    .withColumn("element_rounded_rank", F.rank().over(w3))
)

filtered = (
    metrics
    .filter(F.col("element") < 63)
    .select(
        "id",
        "element_rounded_percent_rank_percent_rank",
        "element_rounded_rank",
        F.round("element_rounded_rank", 1).alias("element_rounded_rank_rounded"),
    )
)

result = (
    filtered
    .select("element_rounded_rank", "element_rounded_rank_rounded")
    .orderBy(
        F.col("element_rounded_rank").asc_nulls_first(),
        "element_rounded_rank_rounded",
    )
)
