from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter((F.col("e.job_title") != "Analyst") & F.month(F.col("e.hire_date")).isin([1, 8, 11]))
    .select(
        F.col("e.manager_id"),
        F.col("e.email"),
        F.col("e.salary"),
        F.col("d.budget"),
    )
)
