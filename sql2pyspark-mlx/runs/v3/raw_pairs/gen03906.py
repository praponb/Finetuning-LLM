from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 22)
    .select(
        "salary",
        "manager_id",
        "hire_date",
        F.substring("email", 1, 4).alias("email_prefix"),
    )
    .orderBy(F.col("email_prefix").desc(), F.col("manager_id").desc())
)
