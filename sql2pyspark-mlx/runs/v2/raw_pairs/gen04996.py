from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.job_title").isin(["Director", "Designer"]) & (F.col("e.salary") >= 112000))
    .select(F.col("e.email"), F.col("e.salary"), F.col("d.location"))
    .orderBy(F.col("salary").desc(), "location")
)
