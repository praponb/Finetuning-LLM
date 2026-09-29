from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") >= 57000)
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("t.salary_range"),
        F.col("t.total_salary"),
    )
)
