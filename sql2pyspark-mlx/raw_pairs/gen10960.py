from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_depts"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.unique_depts") >= 7)
    .select(F.col("d.location"), F.col("ebd.unique_depts"))
    .orderBy(F.col("location").desc())
)
