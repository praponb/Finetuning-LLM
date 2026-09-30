from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull() & F.col("dept_id").isin([10, 21]) & F.col("hire_date").between("2024-06-01", "2025-09-01"))
    .select(
        "is_remote",
        "dept_id",
        "full_name",
        F.trim("email").alias("clean_email"),
        F.upper("full_name").alias("full_name_upper"),
    )
    .orderBy("clean_email")
)
