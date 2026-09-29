from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isNotNull())
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.hire_date"),
        F.col("e.manager_id"),
        F.col("e.email"),
    )
)
