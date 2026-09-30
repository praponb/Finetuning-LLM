from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2024-04-24")
    .select("id", "job_title", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([1, 8]))
        .select("id", "job_title", "manager_id")
    )
    .orderBy("manager_id")
)
