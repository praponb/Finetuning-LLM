from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("job_title") == "Intern")
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ebd.unique_levels"),
        F.col("ebd.avg_salary"),
    )
)
