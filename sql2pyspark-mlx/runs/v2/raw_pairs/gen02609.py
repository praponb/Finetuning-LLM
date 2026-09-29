from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.max_salary") >= 111000)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("t.last_hire_date"),
        F.col("t.max_salary"),
    )
    .orderBy(F.col("budget").desc())
)
