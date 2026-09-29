from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("job_title").isin(["Analyst", "Intern"])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.location") == "Singapore")
    .select(F.col("d.id"), F.col("d.budget"))
    .orderBy(F.col("id").desc())
)
