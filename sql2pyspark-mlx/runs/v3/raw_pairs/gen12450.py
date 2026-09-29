from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 71000)
    .groupBy("job_title")
    .agg(F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"))
    .orderBy("l3_count", "job_title")
    .limit(10)
)
