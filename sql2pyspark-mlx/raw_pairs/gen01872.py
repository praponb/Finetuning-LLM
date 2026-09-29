from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") <= 98000) & (F.col("job_title") == "Director"))
    .select(
        "job_title",
        "is_remote",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.when(F.col("level") == "L5", 1).otherwise(0).alias("is_l5"),
    )
)
