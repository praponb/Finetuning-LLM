from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id", "job_title")
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
    .orderBy(F.col("dept_id").desc(), F.col("engineer_count").desc())
)
