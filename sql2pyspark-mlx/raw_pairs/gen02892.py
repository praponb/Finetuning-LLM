from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Intern")
    .withColumn("full_name_length", F.length("full_name"))
)
