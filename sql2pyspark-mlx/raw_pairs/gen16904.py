from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "email",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
        F.date_format("hire_date", "yyyy-MM-dd").alias("hire_date_label"),
    )
    .orderBy("hire_date_label", "id")
    .limit(5)
)
