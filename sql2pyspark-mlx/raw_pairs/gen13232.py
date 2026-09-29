from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("dept_id")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isNull())
        .select("dept_id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L2", 1)), F.lit(0)), 2).alias("salary_per_l2"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Manager", 1)), F.lit(0)), 2).alias("salary_per_manager"),
    )
    .orderBy(F.col("manager_rows").desc())
)
