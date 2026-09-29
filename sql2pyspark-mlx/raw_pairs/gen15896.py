from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote", "job_title")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("min_salary") >= 138000)
    .orderBy(F.col("avg_salary").desc(), "job_title")
)
