from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([8, 15, 20, 21]) | F.col("salary").between(77000, 82000))
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.max("hire_date").alias("last_hire_date"),
    )
    .orderBy("unique_job_titles", F.col("l5_count").desc())
)
