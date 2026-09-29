from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"))
)
