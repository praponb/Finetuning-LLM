from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select(
        "id",
        "job_title",
        F.floor("salary").alias("salary_whole"),
        F.round("salary", 0).alias("salary_rounded"),
    )
)
