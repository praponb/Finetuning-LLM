from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget").between(2643000, 3356000)).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .select(F.col("e.id"), F.col("e.manager_id"), F.col("e.dept_id"))
    .orderBy(F.col("manager_id").desc(), "dept_id", "id")
    .limit(20)
)
