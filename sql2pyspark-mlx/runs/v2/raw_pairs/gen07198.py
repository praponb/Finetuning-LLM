from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location") == "Singapore").alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.col("e.salary").between(104000, 144000))
    .select(F.col("e.id"), F.col("e.level"), F.col("e.hire_date"))
)
