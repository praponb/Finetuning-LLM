from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.job_title").isin(["Intern", "Manager"]))
    .select(F.col("e.id"), F.col("e.job_title"), F.col("e.salary"), F.col("d.location"))
    .orderBy("salary")
)
