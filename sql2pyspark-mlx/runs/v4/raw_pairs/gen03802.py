from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Manager", "Director", "Engineer"]) & (F.col("dept_id") == 29))
    .withColumn("hire_date_period", F.date_trunc("WEEK", "hire_date"))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)
