from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "job_title",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.round((F.col("salary") * 1.07), 2).alias("salary_scaled"),
    )
)
