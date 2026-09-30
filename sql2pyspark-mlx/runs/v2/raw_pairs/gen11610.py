from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("job_title").isin(["Manager", "Engineer", "Analyst"])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .select(F.col("d.id"), F.col("d.location"), F.col("d.budget"), F.col("d.dept_name"))
    .orderBy(F.col("dept_name").desc(), "id")
    .limit(5)
)
