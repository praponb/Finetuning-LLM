from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(F.col("d.location").isin(["Chiang Mai", "London", "New York"]))
    .select(F.col("d.id"), F.col("d.dept_name"))
    .orderBy("dept_name")
)
