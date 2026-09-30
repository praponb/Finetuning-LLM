from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th"))
    .select(
        "id",
        "job_title",
        "hire_date",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy(F.col("id").desc(), F.col("is_remote_label").desc())
)
