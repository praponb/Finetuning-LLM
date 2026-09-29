from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.hire_date") <= "2024-02-01")
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("e.email"),
        F.col("e.dept_id"),
        F.col("e.hire_date"),
    )
    .orderBy("email")
)
