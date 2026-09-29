from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-11-01", "2024-03-27"))
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
    )
    .orderBy("l1_salary", "job_title")
    .limit(10)
)
