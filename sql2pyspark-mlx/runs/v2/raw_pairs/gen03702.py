from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "grace kim")
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.max_salary") > 70000)
    .select(F.col("d.dept_name"), F.col("t.max_salary"))
    .orderBy("max_salary")
)
