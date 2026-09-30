from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 73000)
    .select("id", "is_remote", "hire_date", F.length("email").alias("email_length"))
)
