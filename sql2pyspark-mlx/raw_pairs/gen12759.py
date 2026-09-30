from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.length(F.col("e.email")) > 13) & F.col("e.hire_date").between("2024-08-01", "2025-07-01"))
    .select(
        F.col("e.manager_id"),
        F.col("e.full_name"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
    .orderBy("location")
)
