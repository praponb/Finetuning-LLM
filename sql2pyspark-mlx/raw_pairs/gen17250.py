from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("location").isNull())
    .withColumn("clean_dept_name", F.trim("dept_name"))
)

result = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= "2024-10-23"), "left_anti")
)
