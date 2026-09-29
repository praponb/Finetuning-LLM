from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
        F.max("salary").alias("max_salary"),
        F.max("hire_date").alias("latest_hire_date"),
    )
    .orderBy(F.col("max_salary").desc())
)
