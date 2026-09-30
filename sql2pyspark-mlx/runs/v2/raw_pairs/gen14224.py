from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@startup.io") & F.col("email").like("%@outlook.com"))
    .select(
        "job_title",
        "full_name",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
