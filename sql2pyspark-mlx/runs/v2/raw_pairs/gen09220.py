from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Intern"]))
    .groupBy("level", "manager_id", "is_remote")
    .agg(F.min("salary").alias("min_salary"))
    .orderBy(F.col("level").desc(), "manager_id", "is_remote")
    .limit(5)
)
