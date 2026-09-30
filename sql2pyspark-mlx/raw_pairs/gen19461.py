from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("dept_id") == 37))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat_ws(",", "email", "job_title").alias("email_job_title_key"),
    )
)

result = (
    cleaned
    .groupBy("dept_id")
    .pivot("job_title", ["Intern", "Designer"])
    .agg(F.max("salary"))
)
