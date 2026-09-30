from pyspark.sql import functions as F

base = df_employees.filter(F.month("hire_date").isin([8, 9, 11]))

job_title_summary = (
    base
    .groupBy("job_title", "is_remote")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    job_title_summary
    .filter(F.col("avg_salary") < 24)
    .select("job_title", "is_remote", "avg_salary")
)
