from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L5", "L2", "L4"]))
    .select("id", "is_remote", "job_title")
    .orderBy(F.col("id").desc())
)
