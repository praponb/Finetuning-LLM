from pyspark.sql import functions as F

cleaned_adjusters = (
    df_adjusters
    .filter((F.upper(F.trim(F.col("adjuster_name"))).like("H%")) | (F.col("level") >= 1))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

scored = (
    cleaned_adjusters
    .filter(F.col("team") != "Health")
    .select(
        "*",
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("level").cast("string")).alias("adjuster_name_ref"),
    )
)

per_group = (
    scored
    .groupBy("level")
    .agg(
        F.sum("level").alias("total_level"),
        F.count("*").alias("n_rows"),
        F.min("level").alias("min_level"),
    )
)

result = (
    per_group
    .select(
        "level",
        "total_level",
        "n_rows",
        "min_level",
        F.round(F.col("level") / F.nullif(F.col("n_rows"), F.lit(0)), 3).alias("level_per_n_rows"),
    )
    .orderBy(F.col("level").desc_nulls_first())
)
