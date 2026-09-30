from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name").isin(["HR", "Engineering", "Finance"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.col("e.manager_id") == 28)
    .select(F.col("e.id"), F.col("e.dept_id"))
    .orderBy(F.col("id").desc())
)
