from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("budget") <= 2014000)
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .withColumn("budget_rounded", F.round("budget", 0))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

remaining_keys = (
    cleaned
    .select("id", "location")
    .exceptAll(
        df_departments
        .filter(F.col("budget").between(1085000, 1627000))
        .select("id", "location")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("location", "id")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name").isin(["Marketing", "Finance", "Legal"]))
        .select("location", "id")
    )
)

kept_rows_departments = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned_departments = (
    kept_rows_departments
    .filter(F.col("dept_name").isNotNull() & F.col("budget_rounded").between(45, 48))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_prefix",
        F.round((F.col("budget_rounded") / F.col("budget")), 2).alias("budget_rounded_per_budget"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_clean"),
    )
)

cleaned_departments_2 = (
    cleaned_departments
    .filter(F.col("budget") >= 2949000)
    .select(
        "*",
        (F.col("budget") * F.col("budget_rounded_per_budget")).alias("budget_times_budget_rounded_per_budget"),
    )
)

result = (
    cleaned_departments_2
    .select(
        "dept_name",
        "location",
        "budget",
        "budget_rounded_per_budget",
        "location_clean",
        F.when(F.col("location").rlike("^(Chiang Mai|Bangkok)$"), "CORE").otherwise("PRIORITY").alias("location_zone"),
    )
    .orderBy("budget", "dept_name")
)
