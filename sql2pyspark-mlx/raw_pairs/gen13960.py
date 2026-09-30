from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget") >= 3883000).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.upper(F.col("e.email")).like("%RAN%"))
    .select(F.col("e.id"), F.col("e.level"), F.col("e.is_remote"))
)
