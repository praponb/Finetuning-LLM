from pyspark.sql import functions as F

result = (
    df_employees
    .select("manager_id", "dept_id")
    .intersect(
        df_employees
        .filter(F.col("hire_date").between("2024-09-01", "2025-03-01"))
        .select("manager_id", "dept_id")
    )
)
