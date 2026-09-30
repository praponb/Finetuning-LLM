from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L1", "L3", "L2"]) & (F.col("job_title") == "Manager"))
    .select(
        "id",
        "dept_id",
        "job_title",
        F.date_trunc("week", "hire_date").alias("hire_date_period"),
        F.concat_ws("/", "email", "full_name").alias("email_full_name_key"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
