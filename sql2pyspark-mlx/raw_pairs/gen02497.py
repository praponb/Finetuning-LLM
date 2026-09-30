from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-08-02").cast("date"))
    .select("*", F.upper(F.trim("email")).alias("email_normalized"))
)

result = (
    base
    .select("manager_id", "job_title")
    .subtract(
        df_employees
        .filter(F.lower("full_name").like("%oah%"))
        .select("manager_id", "job_title")
    )
)
