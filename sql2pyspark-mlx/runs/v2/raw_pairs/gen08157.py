from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L5", "L4"]))
    .select("job_title")
    .distinct()
    .orderBy("job_title")
)
