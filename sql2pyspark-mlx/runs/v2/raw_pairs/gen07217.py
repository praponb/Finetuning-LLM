from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .select(
        "id",
        "is_remote",
        "level",
        "job_title",
        F.substring("full_name", 1, 2).alias("full_name_prefix"),
        F.dayofweek("hire_date").alias("hire_date_dayofweek"),
    )
)
