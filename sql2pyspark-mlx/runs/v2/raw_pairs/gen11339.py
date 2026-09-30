from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("dept_id").isin([4, 17, 20])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(~F.col("d.dept_name").isin(["Marketing", "Engineering", "Sales"]))
    .select(F.col("d.id"), F.col("d.budget"))
    .orderBy(F.col("id").desc())
)
