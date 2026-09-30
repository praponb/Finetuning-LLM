from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.hire_date") <= "2023-03-01")
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("e.hire_date"),
        F.col("e.is_remote"),
    )
    .orderBy("hire_date")
)
