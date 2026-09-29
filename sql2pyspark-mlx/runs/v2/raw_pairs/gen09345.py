from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "inner")
    .filter(F.col("department_stats.salary_range") >= 30)
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("department_stats.salary_range"),
    )
    .orderBy("location", F.col("id").desc())
)
