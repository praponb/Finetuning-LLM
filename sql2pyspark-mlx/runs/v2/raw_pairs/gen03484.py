from pyspark.sql import functions as F

result = (
    df_employees
    .filter(((F.year("hire_date") >= 2025) & (F.col("level") == "L2")) | (F.col("manager_id") == 33))
    .select(
        "job_title",
        "full_name",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
    .orderBy("is_remote_flag")
)
