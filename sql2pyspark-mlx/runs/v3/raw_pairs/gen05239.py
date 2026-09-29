from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.col("full_name").isNotNull() & (F.col("hire_date") <= "2024-12-01")) | F.col("email").like("%@startup.io")))
    .select(
        "id",
        "manager_id",
        "is_remote",
        "dept_id",
        F.round((F.col("salary") * 0.9), 2).alias("salary_scaled"),
    )
    .orderBy(F.col("salary_scaled").desc())
)
