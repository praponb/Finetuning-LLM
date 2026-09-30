from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.count("*").alias("total_count"),
    )
)
