from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Sales", "HR"]), "Priority").otherwise("Other").alias("dept_name_group"),
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
        F.coalesce("location", F.lit("NONE")).alias("location_clean"),
    )
)

kept_keys = (
    filtered
    .select("location")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isin(["HR", "Sales"]))
        .select("location")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["HR", "Legal", "Engineering"]))
        .select("id")
    )
)

reconciled_departments = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled_departments
    .filter(F.col("location").isin(["Chiang Mai", "Singapore"]))
    .withColumn("budget_rounded", F.round("budget", 1))
    .withColumn("is_hr", F.when(F.col("dept_name") == "HR", 1).otherwise(0))
    .withColumn("location_clean_2", F.coalesce("location", F.lit("UNKNOWN")))
)

pivoted = (
    valid
    .groupBy("dept_name")
    .pivot("location", ["London", "Tokyo", "Bangkok"])
    .agg(F.sum("budget_rounded"))
)

result = (
    pivoted
    .select("dept_name", "Tokyo", "Bangkok")
    .orderBy(F.col("Tokyo").asc_nulls_first(), "Bangkok")
)
