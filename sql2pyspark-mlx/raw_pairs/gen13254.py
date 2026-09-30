from pyspark.sql import functions as F

result = (
    df_employees
    .select("email", "hire_date", "manager_id", F.trim("email").alias("clean_email"))
)
