from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("Hen%") & (F.col("hire_date") < "2024-07-01"))
    .select("manager_id", "is_remote", "level", F.lower("email").alias("email_lower"))
    .orderBy(F.col("manager_id").desc())
)
