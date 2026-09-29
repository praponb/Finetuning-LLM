from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"))
    .orderBy(F.col("intern_count").desc())
)
