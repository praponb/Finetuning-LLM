from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("unique_managers") >= 8)
)
