from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(F.col("d.budget").between(3274000, 3657000))
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.location"), F.col("d.dept_name"))
    .orderBy(F.col("location").desc(), "id")
    .limit(5)
)
