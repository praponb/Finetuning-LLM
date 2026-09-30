from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.salary_range") > 40)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.min_hire_date"),
        F.col("t.salary_range"),
    )
    .orderBy("dept_name", "id")
    .limit(20)
)
