from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.length("full_name") >= 13)
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "left")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("department_stats.salary_range"),
        F.col("department_stats.l5_count"),
    )
)
