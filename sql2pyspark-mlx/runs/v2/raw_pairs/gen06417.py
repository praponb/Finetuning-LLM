from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 9)
    .select("job_title", "row_count")
)
