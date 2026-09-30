from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("level") == "L4").alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(F.col("d.budget").between(1601000, 3890000))
    .select(F.col("d.id"), F.col("d.budget"))
)
