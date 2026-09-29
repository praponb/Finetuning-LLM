from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.month("hire_date").isin([2, 4, 10])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.budget") <= 3852000)
    .select(F.col("d.id"), F.col("d.location"), F.col("d.dept_name"), F.col("d.budget"))
)
