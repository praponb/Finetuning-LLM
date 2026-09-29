from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("t.unique_managers"),
        F.col("t.salary_range"),
    )
)
