from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "job_title")
    .agg(F.min("salary").alias("min_salary"))
    .orderBy(F.col("job_title").desc(), "level")
    .limit(50)
)
