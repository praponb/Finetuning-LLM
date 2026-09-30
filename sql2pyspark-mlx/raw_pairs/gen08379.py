from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id").isNull() & (F.col("salary") <= 131000)) | F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.col("is_remote").cast("int").alias("is_remote_flag_2"),
    )
)
