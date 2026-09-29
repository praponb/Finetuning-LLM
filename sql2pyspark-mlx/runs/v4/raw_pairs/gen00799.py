from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("last_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("D"), F.col("d.id") == F.col("D.dept_id"), "left")
    .select(F.col("d.dept_name"), F.col("D.last_hire_date"))
    .orderBy("last_hire_date", "dept_name")
)
