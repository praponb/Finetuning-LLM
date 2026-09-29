from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("salary") < 145000)
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "left")
    .filter(F.col("E.min_salary") > 144000)
    .select(F.col("d.dept_name"), F.col("E.min_salary"))
    .orderBy("min_salary", "dept_name")
)
