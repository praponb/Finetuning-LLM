from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 38)
    .groupBy("job_title", "level")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") >= 70)
    .select("job_title", "level", "salary_range")
    .orderBy(F.col("level").desc(), "job_title")
)
