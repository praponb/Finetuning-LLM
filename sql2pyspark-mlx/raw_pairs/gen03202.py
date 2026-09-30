from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("val"))
)

base = (
    flattened
    .filter(F.col("val") <= 67)
    .select("*", F.last_day("effective_from").alias("effective_from_month_end"))
)

valid = (
    base
    .filter(F.col("val") > 63)
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        "effective_from_month_end",
        F.weekofyear("effective_from").alias("effective_from_weekofyear"),
    )
)

filtered = (
    valid
    .filter(F.year("effective_from_month_end") >= 2025)
    .select(
        "val",
        "effective_from_month_end",
        "effective_from_weekofyear",
        F.coalesce("val", F.lit(0)).alias("val_filled"),
        F.col("effective_from_weekofyear").cast("double").alias("effective_from_weekofyear_str"),
    )
)

result = (
    filtered
    .groupBy(
        F.date_trunc("YEAR", "effective_from_month_end").alias("year_start"),
        F.when(F.col("effective_from_weekofyear") >= 89, "LARGE").otherwise("SMALL").alias("effective_from_weekofyear_tier"),
    )
    .agg(
        F.min("val").alias("min_val"),
        F.round(F.avg("effective_from_weekofyear_str"), 2).alias("avg_effective_from_weekofyear_str"),
        F.percentile_approx("effective_from_weekofyear_str", 0.95).alias("effective_from_weekofyear_str_p95"),
    )
    .orderBy(F.col("effective_from_weekofyear_str_p95").asc_nulls_first())
)
