from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget") >= 2402000).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .select(F.col("e.id"), F.col("e.job_title"), F.col("e.manager_id"))
    .orderBy("id")
    .limit(10)
)
