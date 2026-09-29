from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget") < 3659000).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .select(F.col("e.id"), F.col("e.dept_id"), F.col("e.hire_date"))
    .orderBy("hire_date")
)
