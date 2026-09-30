from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 16) & F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        "full_name",
        F.concat_ws("-", "full_name", "email").alias("full_name_email_key"),
        F.substring("full_name", 1, 2).alias("full_name_prefix"),
    )
)
