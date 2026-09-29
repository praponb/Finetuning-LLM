from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(F.countDistinct("salary").alias("unique_depts"))
    .filter(F.col("unique_depts") < 4)
    .select("job_title", "unique_depts")
    .orderBy("job_title")
)
