from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "hire_date",
        "salary",
        "is_remote",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.col("salary").cast("int").alias("salary_int"),
    )
    .orderBy("is_remote_flag")
)
