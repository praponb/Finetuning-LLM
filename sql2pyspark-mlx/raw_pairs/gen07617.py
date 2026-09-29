from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.budget").between(2799000, 3977000))
    .groupBy(F.col("d.id"), F.col("d.location"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
    .filter(F.col("employee_count") > 2)
)
