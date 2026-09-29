from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Engineer", "Designer"]))
    .select("job_title", "level")
    .distinct()
    .orderBy("level", "job_title")
    .limit(5)
)
