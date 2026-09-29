from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.dept_name") == "Engineering")
    .groupBy(F.col("d.dept_name"))
    .agg(F.min(F.col("e.salary")).alias("min_salary"))
    .orderBy(F.col("min_salary").desc(), "dept_name")
    .limit(10)
)
