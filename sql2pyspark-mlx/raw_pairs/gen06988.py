from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") >= 78000) | F.col("is_remote"))
    .groupBy("level")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.min("salary").alias("min_salary"),
    )
    .orderBy(F.col("unique_job_titles").desc(), F.col("min_salary").desc())
)
