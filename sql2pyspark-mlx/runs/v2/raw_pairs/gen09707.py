from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("salary").between(74000, 121000)).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.budget").isNotNull())
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.location"))
)
