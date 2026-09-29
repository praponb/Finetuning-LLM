from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(~F.col("job_title").isin(["Analyst", "Engineer"])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .select(F.col("d.id"), F.col("d.budget"))
    .orderBy(F.col("budget").desc(), "id")
)
