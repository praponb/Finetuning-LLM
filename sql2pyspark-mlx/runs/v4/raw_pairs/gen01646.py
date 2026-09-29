from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "dept_id")
    .pivot("job_title", ["Engineer", "Intern", "Analyst"])
    .agg(F.avg("salary").alias("value_sum"))
)
