from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.countDistinct("salary").alias("unique_job_titles"))
    .orderBy(F.col("unique_job_titles").desc())
)
