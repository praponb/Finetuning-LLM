from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") >= 148000)
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.min_salary") > 75000)
    .select(F.col("d.budget"), F.col("ds.min_salary"), F.col("ds.first_hire_date"))
)
