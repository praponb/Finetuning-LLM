from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("years_since_effective_from").desc_nulls_last())
w2 = Window.orderBy("years_since_effective_from_percent_rank")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

valid = (
    exploded
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-02-24"))) & F.col("effective_from").between("2024-06-01", "2025-10-18"))
    .select(
        "*",
        F.length("currency").alias("currency_length"),
        (F.datediff(F.current_date(), "effective_from") / 365).alias("years_since_effective_from"),
    )
)

enriched = (
    valid
    .filter(F.col("effective_from").between("2023-06-01", "2024-03-01"))
    .select(
        "id",
        "effective_from",
        F.percent_rank().over(w).alias("years_since_effective_from_percent_rank"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "id",
        "effective_from",
        F.rank().over(w2).alias("years_since_effective_from_percent_rank_rank"),
    )
)

result = (
    enriched_price_tiers
    .select("id", "effective_from")
    .orderBy(F.col("id").desc_nulls_last(), F.col("effective_from").desc())
)
