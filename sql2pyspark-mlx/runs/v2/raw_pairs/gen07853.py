from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        F.lower("email").alias("email_lower"),
    )
    .orderBy("full_name", "id")
    .limit(5)
)
