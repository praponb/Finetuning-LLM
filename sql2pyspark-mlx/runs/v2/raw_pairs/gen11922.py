from pyspark.sql import functions as F

result = (
    df_employees
    .select("job_title", "level")
    .distinct()
    .orderBy("level")
)
