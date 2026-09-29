from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-05-01", "2025-09-01") & F.lower("full_name").like("%any%"))
    .agg(F.countDistinct("salary").alias("unique_job_titles"))
    .orderBy(F.col("unique_job_titles").desc())
)
