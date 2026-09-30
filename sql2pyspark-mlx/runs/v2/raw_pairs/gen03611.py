from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([17, 18, 31]))
    .groupBy("level")
    .agg(F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"))
)
