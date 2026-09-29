from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("salary") >= 59000)
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "left")
    .select(F.col("d.location"), F.col("E.total_salary"), F.col("E.row_count"))
    .orderBy(F.col("location").desc())
)
