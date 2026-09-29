from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.is_remote") | F.col("d.dept_name").isin(["Legal", "Engineering"]))
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("e.manager_id"),
        F.col("e.job_title"),
    )
)
