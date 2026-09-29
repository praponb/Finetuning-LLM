from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
    )
    .orderBy("first_hire_date")
)
