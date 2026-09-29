from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("email").isNotNull() & F.col("job_title").isin(["Analyst", "Intern", "Director"])) | F.col("is_remote"))
    .groupBy("is_remote")
    .agg(F.min("hire_date").alias("first_hire_date"))
    .orderBy("first_hire_date")
)
