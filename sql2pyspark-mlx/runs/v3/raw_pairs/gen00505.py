from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.job_title") == "Intern")
    .groupBy(F.col("d.dept_name"))
    .agg(F.avg(F.col("e.salary")).alias("avg_salary"))
    .filter(F.col("avg_salary") >= 72000)
)
