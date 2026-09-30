from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 24, 33, 40]) & F.col("dept_id").isin([25, 26, 32]))
    .select(
        "id",
        "manager_id",
        "job_title",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)
