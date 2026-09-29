from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy("dept_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.cnt") > 8)
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("d.location"), F.col("ebd.cnt"))
    .orderBy("dept_name")
)
