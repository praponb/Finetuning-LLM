from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 2) & F.col("dept_id").isin([2, 11, 20, 21]))
    .select("salary", "job_title", F.upper("job_title").alias("job_title_upper"))
    .orderBy(F.col("salary").desc(), F.col("job_title_upper").desc())
)
