from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "email",
        "job_title",
        "hire_date",
        F.substring("full_name", 1, 4).alias("full_name_prefix"),
    )
    .orderBy("full_name_prefix")
)
