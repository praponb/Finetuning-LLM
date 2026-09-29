from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .groupBy("is_remote", "job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
        F.count("*").alias("employee_count"),
    )
    .filter(F.col("intern_count") > 24)
    .orderBy("intern_count", F.col("employee_count").desc())
)
