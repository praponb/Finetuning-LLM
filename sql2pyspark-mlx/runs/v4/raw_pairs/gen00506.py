from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget").between(2217000, 2885000)).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .select(F.col("e.id"), F.col("e.salary"), F.col("e.email"))
    .orderBy("email", F.col("salary").desc(), "id")
    .limit(50)
)
