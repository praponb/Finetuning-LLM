from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-11-15", "2025-05-16"))
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
    )
    .orderBy(F.col("analyst_salary").desc(), F.col("job_title").desc())
)
