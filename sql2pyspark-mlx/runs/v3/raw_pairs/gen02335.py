from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Intern", "Manager"]) & F.col("is_remote"))
    .select(
        "manager_id",
        "full_name",
        F.date_sub("hire_date", 4).alias("hire_date_minus_4d"),
    )
    .orderBy(F.col("hire_date_minus_4d").desc(), "full_name")
)
