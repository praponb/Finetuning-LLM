from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
        F.min("salary").alias("min_salary"),
    )
    .orderBy(F.col("min_salary").desc())
)
