from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["New York", "Tokyo", "Singapore"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.job_title") == "Director")
    .select(F.col("e.id"), F.col("e.full_name"), F.col("e.email"), F.col("e.level"))
    .orderBy("id", F.col("email").desc())
)
