from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "right")
    .groupBy(F.col("d.dept_name"))
    .agg(
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.min(F.col("e.hire_date")).alias("first_hire_date"),
    )
    .orderBy("salary_range", "dept_name")
    .limit(20)
)
