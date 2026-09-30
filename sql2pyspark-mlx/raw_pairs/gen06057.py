from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("element"))
)

valid = (
    exploded
    .filter(F.year("effective_from") >= 2023)
    .withColumn("element_rounded", F.round("element", 0))
)

base = (
    valid
    .filter(F.col("effective_from").between("2023-08-01", "2025-07-27"))
    .select(
        "*",
        F.round((F.col("element_rounded") * 1.1), 2).alias("element_rounded_adjusted"),
        (F.col("element_rounded") + F.col("element")).alias("element_rounded_plus_element"),
    )
)

kept_keys = (
    base
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.month("effective_from").isin([5, 6, 8]))
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("element_rounded_plus_element", [0.25, 0.5, 0.75]).alias("element_rounded_plus_element_quartiles"),
        F.min("element").alias("min_element"),
    )
    .filter(F.col("min_element") > 0)
)
