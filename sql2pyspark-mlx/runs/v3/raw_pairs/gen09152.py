from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget") > 2210000).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .select(F.col("e.id"), F.col("e.level"), F.col("e.hire_date"), F.col("e.email"))
    .orderBy("hire_date", F.col("id").desc())
)
