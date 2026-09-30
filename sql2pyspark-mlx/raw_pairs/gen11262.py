from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("years_since_effective_from").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

base = (
    lines
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-06-20"))) & (F.year("effective_from") == 2025))
    .select(
        "*",
        (F.datediff(F.current_date(), "effective_from") / 365.25).alias("years_since_effective_from"),
    )
)

remaining_keys = (
    base
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("effective_from") < "2025-08-25")
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "effective_from",
        "val",
        F.percent_rank().over(w).alias("years_since_effective_from_percent_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("val") < 75)
    .withColumn("years_since_effective_from_percent_rank_whole", F.floor("years_since_effective_from_percent_rank"))
)

valid = (
    filtered
    .filter(F.col("effective_from") > "2025-06-01")
    .select(
        "effective_from",
        "val",
        "years_since_effective_from_percent_rank",
        "years_since_effective_from_percent_rank_whole",
        F.coalesce("years_since_effective_from_percent_rank_whole", F.lit(0)).alias("years_since_effective_from_percent_rank_whole_filled"),
    )
)

cleaned = (
    valid
    .filter(F.year("effective_from") == 2024)
    .select("*", F.date_add("effective_from", 6).alias("effective_from_plus_6d"))
)

report = (
    cleaned
    .select(
        "effective_from",
        "years_since_effective_from_percent_rank",
        "years_since_effective_from_percent_rank_whole",
        "years_since_effective_from_percent_rank_whole_filled",
        "effective_from_plus_6d",
    )
    .orderBy(F.col("effective_from").asc_nulls_first())
)

result = (
    report
    .select(
        "years_since_effective_from_percent_rank",
        "years_since_effective_from_percent_rank_whole",
        "years_since_effective_from_percent_rank_whole_filled",
        "effective_from_plus_6d",
    )
    .orderBy(F.col("effective_from_plus_6d").desc_nulls_last())
)
