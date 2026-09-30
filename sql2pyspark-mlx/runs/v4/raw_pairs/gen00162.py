from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("dept_id").isin([2, 17, 31]))
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(F.col("d.budget"), F.col("ebd.max_salary"))
)
