from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") < 59)
    .orderBy("salary_range")
)
