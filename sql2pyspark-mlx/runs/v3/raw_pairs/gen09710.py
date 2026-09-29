from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.dept_name") == "Finance")
    .select(
        F.col("employees.id"),
        F.col("employees.email"),
        F.col("employees.level"),
        F.col("employees.job_title"),
        F.col("departments.location"),
        F.regexp_extract(F.col("employees.email"), "@(.+)$", 1).alias("email_domain"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
