from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") < 10) & F.month("hire_date").isin([3, 7, 10]))
    .select("id", "job_title", "dept_id", F.length("level").alias("level_length"))
    .orderBy("level_length")
)
