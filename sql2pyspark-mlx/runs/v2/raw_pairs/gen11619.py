from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("dept_id").isin([10, 13, 26, 35])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .select(F.col("d.id"), F.col("d.dept_name"))
    .orderBy(F.col("id").desc())
)
