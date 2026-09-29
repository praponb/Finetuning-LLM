from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.dept_name").isin(["Engineering", "Legal", "HR"]) & F.col("d.budget").between(1715000, 3713000))
    .select(F.col("e.id"), F.col("e.salary"), F.col("d.budget"), F.col("d.location"))
)
