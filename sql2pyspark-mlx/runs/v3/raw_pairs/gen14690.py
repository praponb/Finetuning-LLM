from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.lower(F.trim("email")) == "grace.kim@gmail.com").alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(F.col("d.dept_name").isin(["HR", "Engineering"]))
    .select(F.col("d.id"), F.col("d.dept_name"))
)
