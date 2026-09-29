from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "dept_id")
    .pivot("job_title", ["Engineer", "Intern"])
    .agg(F.sum("salary").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
