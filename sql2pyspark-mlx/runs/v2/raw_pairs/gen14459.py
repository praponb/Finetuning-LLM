from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Director")
    .orderBy(F.col("full_name").desc(), "id")
    .limit(3)
)
