from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%smith%") & ~F.col("is_remote"))
    .select("level", F.lower("email").alias("email_lower"))
    .orderBy(F.col("email_lower").desc())
)
