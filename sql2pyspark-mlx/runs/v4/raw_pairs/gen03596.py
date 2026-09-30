from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.location").isin(["Tokyo", "London", "New York"]) & F.col("d.budget").isNull())
    .select(
        F.col("d.location"),
        F.col("e.manager_id"),
        F.col("e.is_remote"),
        F.col("e.job_title"),
    )
)
