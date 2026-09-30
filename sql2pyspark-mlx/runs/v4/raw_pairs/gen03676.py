from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "is_remote", "email", F.lower("level").alias("level_lower"))
)
