from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "level",
        "full_name",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.col("is_remote").cast("int").alias("is_remote_flag_2"),
    )
)
