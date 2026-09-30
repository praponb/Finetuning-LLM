from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget") < 3635000).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.col("e.manager_id").isin([5, 40]))
    .select(F.col("e.id"), F.col("e.hire_date"))
    .orderBy("id", F.col("hire_date").desc())
)
