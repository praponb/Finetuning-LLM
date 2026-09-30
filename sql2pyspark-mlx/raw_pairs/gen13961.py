from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("id", "location")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isNotNull())
        .select("id", "location")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("location").isNotNull() & (F.col("location").eqNullSafe(F.lit("Berlin"))))
    .withColumn("budget_level", F.when(F.col("budget") >= 3896000, "High").when(F.col("budget") >= 1744000, "Medium").otherwise("Low"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

eligible = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").like("%Mia%")), "left_anti")
)

base_departments = (
    eligible
    .filter(F.col("budget") >= 2154000)
    .select(
        "*",
        F.round("budget", 0).alias("budget_rounded"),
        F.coalesce("location", F.lit("NONE")).alias("location_clean"),
    )
)

result = (
    base_departments
    .select("location", "budget", "budget_rounded")
    .unpivot(
        ids=["location"],
        values=["budget", "budget_rounded"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
