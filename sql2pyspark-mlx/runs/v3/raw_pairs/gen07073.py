from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", "is_remote")
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") > 84000)
    .orderBy(F.col("job_title").desc())
)
