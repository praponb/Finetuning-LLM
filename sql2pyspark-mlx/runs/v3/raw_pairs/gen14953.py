from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("t.max_salary"),
        F.col("t.first_hire_date"),
    )
    .orderBy(F.col("dept_name").desc(), "first_hire_date")
)
