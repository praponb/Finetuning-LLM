from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("dept_id").isin([8, 13, 27])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .select(F.col("d.id"), F.col("d.budget"))
    .orderBy(F.col("id").desc())
)
