from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull() | F.col("hire_date").between("2023-07-01", "2023-08-05"))
    .withColumn("job_title_lower", F.lower("job_title"))
    .withColumn("email_length", F.length("email"))
)
