from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Analyst", "Manager"]) & F.col("email").isNull() & F.col("email").like("%@outlook.com"))
    .select("id", "full_name", F.trim("full_name").alias("clean_full_name"))
    .orderBy("clean_full_name", F.col("id").desc())
)
