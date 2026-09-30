from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([17, 28, 37]))
    .select(
        "job_title",
        "email",
        "dept_id",
        F.concat("email", F.lit("/"), "full_name").alias("email_full_name"),
    )
)
