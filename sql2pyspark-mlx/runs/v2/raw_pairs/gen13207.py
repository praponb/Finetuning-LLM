from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "outer")
    .groupBy(F.col("departments.dept_name"))
    .agg(F.sort_array(F.collect_set(F.col("employees.level"))).alias("level_list"))
    .orderBy(F.col("dept_name").desc())
)
