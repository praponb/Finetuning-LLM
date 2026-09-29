from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter((~F.col("dept_name").eqNullSafe(F.lit("Marketing"))) & F.col("budget").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 1.07), 2).alias("adjusted_budget"),
        F.coalesce("dept_name", F.lit("N/A")).alias("resolved_dept_name"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.hire_date")), "left_anti")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") >= F.col("s.hire_date")), "left_semi")
)

remaining_keys = (
    kept_departments
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("location").isNotNull())
        .select("id")
    )
)

reconciled = (
    kept_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_departments_2 = (
    reconciled
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.hire_date")), "left_anti")
)

kept_departments_3 = (
    kept_departments_2
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") < "2024-06-22"), "left_semi")
)

summary = (
    kept_departments_3
    .groupBy(
        F.when(F.col("adjusted_budget") >= 81, "LARGE").otherwise("SMALL").alias("adjusted_budget_tier"),
    )
    .agg(
        F.sum("adjusted_budget").alias("total_adjusted_budget"),
        F.percentile_approx("adjusted_budget", 0.9).alias("adjusted_budget_p90"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Bangkok", 1)), F.lit(0)), 2).alias("budget_per_bangkok"),
        F.count("*").alias("num_rows_2"),
        F.round(F.sum("adjusted_budget") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("adjusted_budget_per_tokyo"),
    )
)

result = (
    summary
    .select(
        "adjusted_budget_tier",
        "total_adjusted_budget",
        "num_rows",
        "budget_per_bangkok",
        "num_rows_2",
        "adjusted_budget_per_tokyo",
    )
    .orderBy(F.col("adjusted_budget_per_tokyo").asc_nulls_last())
)
