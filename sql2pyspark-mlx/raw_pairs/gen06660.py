from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.max("hire_date").alias("latest_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .filter(F.col("dt.salary_range") > 51)
    .select(
        F.col("d.dept_name"),
        F.col("dt.salary_range"),
        F.col("dt.latest_hire_date"),
    )
)
