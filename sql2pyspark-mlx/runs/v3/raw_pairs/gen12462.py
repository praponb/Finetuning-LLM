from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("e.salary"),
        F.col("e.dept_id"),
        F.col("e.email"),
    )
    .orderBy("salary", "email")
)
